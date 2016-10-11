'''
Google spreadsheet wrapper
'''
import re
import gdata.docs.service
import gdata.docs.data
import gdata.spreadsheet.service

from process.globals import config

# TODO: ExecuteBatch; 2-leg oauth
# TODO: cache rows locally, operate and then flush


def authenticate(client):
    # client.SetOAuthInputParameters(
    #     gdata.auth.OAuthSignatureMethod.HMAC_SHA1,
    #     consumer_key=config.gdocs['consumer_key'],
    #     consumer_secret=config.gdocs['consumer_secret'],
    #     two_legged_oauth=True,
    #     requestor_id=config.gdocs['email']
    # )
    client.ClientLogin(
        config.gdocs['email'],
        config.gdocs['passwd'],
        config.app_name
    )
    client.ssl = True


def new_doc(title):
    '''
    return doc_key
    '''
    client = gdata.docs.service.DocsService(email=config.gdocs['email'], source=config.app_name)
    authenticate(client)

    # entry = gdata.docs.data.Resource(type='spreadsheet', title=title, collection='bot: FR')
    entry = client.Upload(
        gdata.MediaSource(
            file_name=title,
            content_type='application/x-vnd.oasis.opendocument.spreadsheet',
            content_length=0
        ),
        title
    )

    return entry.id.text.rsplit('%3A')[-1]


class Spreadsheet(object):
    def __init__(self, doc=None):
        self.client = gdata.spreadsheet.service.SpreadsheetsService(source=config.app_name)
        authenticate(self.client)

        if doc:
            if not hasattr(doc, 'doc_key'):
                doc = GDocId(doc_url=doc)
            self.doc_key = doc.doc_key
            self.worksheet_id = doc.worksheet_id
        else:
            self.doc_key = new_doc('test1')

        if self.worksheet_id is None:
            self.worksheet_id = self.default_worksheet()

    def default_worksheet(self):
        '''
        return worksheet id
        '''
        wk_feed = self.client.GetWorksheetsFeed(self.doc_key)
        return wk_feed.entry[0].id.text.rsplit('/')[-1]

    def render_headers(self, columns):
        # TODO: set extension cell type and format
        for i, name in enumerate(columns, 1):
            cur = self.get_cell((1, i))
            if cur and cur != name:
                raise Exception("Unexpected header in location (%d, %d): %s" % (1, i, cur,))
            self.client.UpdateCell(1, i, name, self.doc_key, self.worksheet_id)

    def num_rows(self):
        feed = self.client.GetListFeed(self.doc_key, wksht_id=self.worksheet_id)
        # FIXME: race condition
        return len(feed.entry) + 1

    def append_row(self, row):
        rendered = {}
        for key, e in row.items():
            if e is None:
                e = 'none'
            if not hasattr(e, 'decode'):
                e = str(e)
            rendered[key] = e
        self.client.InsertRow(rendered, self.doc_key, self.worksheet_id)

    def update_row(self, props, index=None, matching=None):
        if matching:
            # TODO
            # if len(matches) > 1:
            #   raise Exception
            pass
        feed = self.client.GetListFeed(self.doc_key, wksht_id=self.worksheet_id)
        entry = feed.entry[index - 1]
        for k, v in props.items():
            if k in entry.custom:
                entry.custom[k].text = str(v)
        for a_link in entry.link:
            if a_link.rel == 'edit':
                self.client.Put(entry, a_link.href)

    def set_cell(self, addr, data):
        self.client.UpdateCell(addr[0], addr[1], data, self.doc_key, self.worksheet_id)

    def get_cell(self, addr):
        feed = self.client.GetCellsFeed(
            self.doc_key,
            wksht_id=self.worksheet_id,
            cell=self.rc_addr(addr)
        )
        return feed.text

    def get_row(self, row):
        feed = self.client.GetListFeed(self.doc_key, wksht_id=self.worksheet_id)
        if row > len(feed.entry):
            return None
        ret = {}
        for key, value in feed.entry[row - 1].custom.items():
            ret[key] = value.text
        return ret

    def rc_addr(self, addr):
        return "R%dC%d" % (addr[0], addr[1],)

    def get_all_rows(self):
        '''
        Dump entire spreadsheet and return as a list of dicts
        '''
        feed = self.client.GetListFeed(self.doc_key, wksht_id=self.worksheet_id)
        for line in feed.entry:
            row = {}
            for key, value in line.custom.items():
                row[key] = value.text
            yield row


class GDocId(object):
    def __init__(self, doc_url=None, doc_key=None, worksheet_id=None):
        if doc_url:
            matches = re.search(r'key=([^#&]+)#gid=(\d+)', doc_url)
            if matches:
                self.doc_key = matches.group(1)
                self.worksheet_id = int(matches.group(2)) + 1
            else:
                raise Exception("Could not parse URL: %s" % doc_url)
        else:
            self.doc_key = doc_key
            self.worksheet_id = worksheet_id

    def __repr__(self):
        # FIXME doc types
        return "https://docs.google.com/spreadsheet/ccc?key=%s#gid=%d" % (self.doc_key, self.worksheet_id - 1)
