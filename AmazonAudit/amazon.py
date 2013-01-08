#!/usr/bin/python

import urllib
from urlparse import urlparse, parse_qsl, ParseResult, urlunparse
from hashlib import sha256
import hmac
from base64 import b64encode
from datetime import datetime
from xml.dom.minidom import parse
import dateutil.parser

class Amazon:
    """Has the following attributes:
        self.awsEndpoint
        self.awsAccessKey
        self.awsSecret
    """

    AWS_FPS_VERSION_1 = '2010-08-28'

    FPSOperation = [
        'Pay',
        'Refund',
        'Settle',
        'SettleDebt',
        'WriteOffDebt',
        'FundPrepaid',
        'Reserve',
    ]

    TransactionStatus = [
        'Cancelled',
        'Failure',
        'Pending',
        'Reserved',
        'Success'
    ]

    def __init__(self, awsEndpoint, awsAccessKey, awsSecret):
        self.awsEndpoint = awsEndpoint
        self.awsAccessKey = awsAccessKey
        self.awsSecret = awsSecret

    def doTransaction(self, action, version, params):
        """Perform the requested AWS action.

         -- Parameters
         action  - AWS action name
         version - API version string implemented
         params  - Dictionary of parameters.

         -- Returns
         A DOM tree object. May raise an exception if AWS returns error nodes
        """
        params.update({
            'Action': action,
            'Version': version,
        })
        reqParams = self.signRequest('POST', params)
        req = urllib.urlopen(self.awsEndpoint, urllib.urlencode(reqParams))
        dom = parse(req)
        errors = dom.getElementsByTagName("Error")
        if len(errors) != 0:
            errInfo = []
            for error in errors:
                errInfo.append(
                    error.getElementsByTagName("Code")[0].firstChild.nodeValue + ': ' +
                    error.getElementsByTagName("Message")[0].firstChild.nodeValue
                )

            errStr = errInfo[0]
            if len(errInfo) > 1:
                errStr += " also %s" % (", ".join(errStr[1:]))
            raise AmazonException(errStr)

        return dom

    def signRequest(self, httpMethod, requestParams):
        """ Sign an amazon request per http://docs.amazonwebservices.com/AmazonFPS/latest/FPSAccountManagementGuide/APPNDX_GeneratingaSignature.html
         httpMethod - GET/POST
         requestUri - Endpoint that will be accepting the signature
         requestParams - Dictionary of parameters
         secretKey - AWS secret key

         returns - Signed URL if method is GET, otherwise list of parameters
        """

        if httpMethod not in ['GET','POST']:
            raise Exception('Invalid HTTP Method')

        # Process the requestURI
        requestUriParts = urlparse(self.awsEndpoint)

        # 1a) Take the inbound dict and sort it; combine with requestUriParts as required
        paramList = requestParams.items()
        if httpMethod is 'GET':
            paramList += parse_qsl(requestUriParts.query)

        # Add other signature parts
        paramList += [
            ('AWSAccessKeyId', self.awsAccessKey),
            ('SignatureVersion', '2'),
            ('SignatureMethod', 'HmacSHA256'),
            ('Timestamp', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
        ]
        paramList.sort()

        # 1b) RFC 3986 Encode
        # 1c) Create parameters by = separation
        # 1d) Separate params by &
        reqString = []
        if len(paramList) > 0:
            for k,v in paramList:
                reqString.append("%s=%s" % (urllib.quote(str(k), ''), urllib.quote(str(v), '')))
            reqString = "&".join(reqString)
        else:
            reqString = ''

        # 2) Create the rest of the signing string
        signString = "%s\n%s\n%s\n%s" % (
            httpMethod,
            requestUriParts.netloc,
            requestUriParts.path or '/',
            reqString
        )

        # 3) HMAC sign with SHA256
        # 4) Format as base64
        h = hmac.new(self.awsSecret, signString, sha256)
        sig = b64encode(h.digest())
        paramList += [('Signature', sig)]

        # 5) This then becomes Signature=
        if httpMethod is 'GET':
            query = urlunparse(ParseResult(
                requestUriParts.scheme,
                requestUriParts.netloc,
                requestUriParts.path,
                '',
                reduce(lambda x,y: '%s&%s=%s' % (x, str(y[0]), urllib.quote(str(y[1]), '')), paramList, '')[1:],
                ''
            ))
        else:
            query = paramList

        return query

    def getTransaction(self, transactionId):
        """Get detailed information on an Amazon Payments transaction.

        -- Parameters
        transactionId - the transaction ID given by amazon that uniquely identifies it

        -- Returns
        Dictionary of values
        """
        params = {
            'TransactionId':transactionId
        }
        dom = self.doTransaction( 'GetTransaction', Amazon.AWS_FPS_VERSION_1, params)

        return self.treeToDict(dom.getElementsByTagName("Transaction")[0])

    def getAccountActivity(self, startDate, endDate = None, fpsOperation = None, status = None, returnNext = False, limit = 0):
        """Obtains all account activity matching the given filters.

         -- Parameters
         startDate      - Datetime or string to obtain transactions before
         endDate        - Datetime or string to obtain transactions after
         fpsOperation   - Must be one of Amazon.FPSOperation
         status         - Must be one of Amazon.TransactionStatus
         returnNext     - If True, the function will not continuously call the API. Instead it will return a tuple
                            where the first element is next state date to retrieve from AWS. The second element are
                            the retrieved transaction objects
         limit          - Lower bound is 20 (or 0 for no bound)

         -- Returns
         A list of returned transactions. (Unless returnNext is True)
        """
        if not isinstance(startDate, datetime):
            startDate = dateutil.parser.parse(startDate)
        if (endDate is not None) and (type(endDate) != datetime):
            endDate = dateutil.parser.parse(endDate)

        params = {
            'StartDate': startDate.isoformat(),
            'SortOrderByDate': 'Ascending',
        }
        if endDate is not None:
            params['EndDate'] = endDate.isoformat()
        if fpsOperation is not None:
            if fpsOperation in Amazon.FPSOperation:
                params['FPSOperation'] = fpsOperation
            else:
                raise AmazonException('Provided fpsOperation (%s) argument not valid.' % fpsOperation)
        if status is not None:
            if status in Amazon.TransactionStatus:
                params['TransactionStatus'] = status
            else:
                raise AmazonException('Provided status (%s) argument not valid.' % status)
        if (limit != 0) and (limit < 200):
            # AmazonAWS imposes a minimum batch size limit of 20 or it will throw an error
            params['MaxBatchSize'] = max(20, limit)
        elif limit < 0:
            raise AmazonException('Cannot have a negative limit argument')
        else:
            params['MaxBatchSize'] = 200

        dom = self.doTransaction('GetAccountActivity', Amazon.AWS_FPS_VERSION_1, params)
        dom = self.treeToDict(dom.getElementsByTagName("GetAccountActivityResult")[0])

        # Little bit of cleanup
        if 'StartTimeForNextTransaction' in dom:
            nextStart = dom['StartTimeForNextTransaction']
        else:
            nextStart = None

        if 'Transaction' not in dom:
            # No transactions :'(
            dom['Transaction'] = []
        elif 'TransactionId' in dom['Transaction']:
            # A single transaction which needs to be listafied
            dom['Transaction'] = [dom['Transaction']]

        if returnNext:
            # This behaviour allows us to chain things together to get everything
            return nextStart, dom['Transaction']
        else:
            # Chain the requests until we get a none
            transactions = dom['Transaction']

            while (nextStart is not None) and ((limit == 0) or (len(transactions) < limit)):
                if limit != 0:
                    sublimit = max(20, limit - len(transactions))
                else:
                    sublimit = 200

                (nextStart, tlist) = self.getAccountActivity(nextStart, endDate, fpsOperation, status, True, sublimit)
                transactions += tlist
                print("%s - %s" % (nextStart, len(transactions)))

        return transactions

    def treeToDict(self, domTree):
        """Convert a DOM tree to something a little more useful. Assumes that tree elements have no attributes

        -- Parameters
        domTree - the DOMNode object to turn into a dictionary
        """
        tree = {}

        node = domTree.firstChild
        while node is not None:
            if (node.firstChild is not None) and (node.firstChild.nodeType == node.TEXT_NODE):
                tree[node.localName] = node.firstChild.nodeValue
            else:
                ntree = self.treeToDict(node)
                if node.localName in tree:
                    if hasattr(tree[node.localName], 'append'):
                        tree[node.localName].append(ntree)
                    else:
                        tree[node.localName] = [
                            tree[node.localName],
                            ntree
                        ]
                else:
                    tree[node.localName] = ntree
            node = node.nextSibling
        return tree

class AmazonException(Exception):
    pass
