#!/usr/bin/env python3
# coding=utf-8

"""
Generate a PHP file with Locale data in it from CLDR data. Run with --help for all options.
"""

from optparse import OptionParser
import xml.etree.ElementTree as ET
import os
import codecs


def parseCLDR(cldr_path):
    """Parse CLDR XML files into currency and locale data
    Returns a set of: (
        cldr date
        {currencyIso: {digits, rounding}},
        {locale: {positive_currency_str, negative_currency_str, grouping: []}}
        {locale: {group_char, decimal_char}}
    )

    In the currency strings: $1 is the symbol, $2 is the formatted number
    If the group_char is '', there is no grouping
    In grouping, if there is multiple elements then the apply the groups from the decimal point forwards
    """

    # === Obtain currency data information (digits, rounding)
    currencyData = {}
    supData = ET.parse(cldr_path + 'supplemental/supplementalData.xml').getroot()

    for child in supData.findall("./currencyData/fractions/*"):
        code = child.attrib['iso4217']
        digits = int(child.attrib['digits'])
        rounding = int(child.attrib['rounding'])

        if code == 'DEFAULT':
            # Check some assumptions
            if digits != 2 and rounding != 0:
                print("Assumptions did not hold on currency data default digits and rounding!")
                exit(1)
        else:
            currencyData[code] = {'digits': digits, 'rounding': rounding}

    # --- Get the CLDR version/date
    cldrVersion = supData.find("./version").attrib['number'] + " - " + supData.find("./generation").attrib['date']

    # === Now, for each locale... ===
    localeNumericFormat = {}
    localeSymbols = {}

    for filename in os.listdir(cldr_path + 'main/'):
        locale = filename[:-4]
        data = ET.parse(cldr_path + "main/%s" % filename).getroot()

        # --- ... information on the symbols used ---
        decimalChar = data.find("./numbers/symbols[@numberSystem='latn']/decimal")
        groupChar = data.find("./numbers/symbols[@numberSystem='latn']/group")

        if decimalChar is not None and groupChar is not None:
            localeSymbols[locale] = {'decimal_char': decimalChar.text, 'group_char': groupChar.text}
        elif decimalChar is not None:
            localeSymbols[locale] = {'decimal_char': decimalChar.text, 'group_char': ','}
        elif groupChar is not None:
            localeSymbols[locale] = {'decimal_char': '.', 'group_char': groupChar.text}

        # --- ... information on number and currency formatting ---
        patternNode = data.find(
            "./numbers/currencyFormats[@numberSystem='latn']/currencyFormatLength/currencyFormat/pattern"
        )
        if patternNode is not None:
            pattern = patternNode.text
            if len(pattern.split(';')) == 2:
                localeNumericFormat[locale] = extractNumericLocale(locale, pattern.split(';')[0], pattern.split(';')[1])
            else:
                localeNumericFormat[locale] = extractNumericLocale(locale, pattern)

    return cldrVersion, currencyData, localeNumericFormat, localeSymbols


def extractNumericLocale(locale, pPattern, nPattern=None):
    """Extract grouping char, decimal char, digit grouping, positive, and negative format strings from a pattern
    pPattern -- the positive pattern
    nPattern -- the negative pattern if given
    """
    # Replace a unicode character with something more MediaWiki
    pPattern = pPattern.replace('¤', '$1')
    if nPattern is not None:
        nPattern = nPattern.replace('¤', '$1')

    # Work from the positive pattern first; these come in like ¤ #,##,##0.00 where ¤ is the currency symbol
    # Find the start and end of the numeric pattern -- this seems to always start with # and end with 0
    # We know that , and . are placeholders that should always exist
    start = pPattern.find('#')
    end = pPattern.rfind('0')

    # Find the decimal char
    decimalLoc = pPattern.find('.')
    if decimalLoc == -1:
        # The assumption being it has one...
        print("Locale %s breaks the decimal separator assumption! Pattern: %s" % (locale, pPattern))
        exit(1)

    # Get the grouping, the first character we should see after the first # should be the separator
    # ... unless we don't have group separators...
    groupChar = pPattern[start + 1]
    grouping = []
    if groupChar not in {',', '0'}:
        print("Locale %s breaks the group separator assumption! Pattern: %s" % (locale, pPattern))
        exit(1)
    elif groupChar == '0':
        # No group separator...
        groupChar = ''
    else:
        # Do we have another group separator?
        secondSepLoc = pPattern.find(groupChar, start + 2, end)
        if secondSepLoc != -1:
            # Odd grouping
            grouping.append(secondSepLoc - start - 2)
            # If we have another one it's another violation of an assumption
            if pPattern.find(groupChar, secondSepLoc + 2) != -1:
                print("Locale %s breaks the grouping assumption! Pattern: %s" % (locale, pPattern))
                exit(1)
        else:
            # Only one separator
            secondSepLoc = start + 1

        # What's the grouping between the second group separator and the decimal separator
        grouping.append(decimalLoc - secondSepLoc - 1)

    # Now sub out all this numeric string mess
    numPattern = pPattern[start:end + 1]
    pPattern = pPattern.replace(numPattern, '$2')

    # and construct the nPattern
    if nPattern is not None:
        nPattern = nPattern.replace(numPattern, '$2')
    else:
        nPattern = '-' + pPattern

    return {
        'positive_currency_str': pPattern,
        'negative_currency_str': nPattern,
        'grouping': grouping
    }


def outputData(data, outfile, className, namespace=None):
    """Generates a PHP data file"""

    skeleton = """<?php %(namespace)s
/**
 * This file was automatically generated, do not edit! Use
 *   https://git.wikimedia.org/tree/wikimedia%%2Ffundraising%%2Ftools.git/HEAD/CldrParser
 *   to update this file when required.
 *
 * input: CLDR data file, %(cldrVersion)s
 */
class %(className)s {
    /** @var array array(ISO code => array(decimal digits, rounding digits)) */
    public static $currencyData = array(
        %(currencyData)s
    );

    /** @var array array(ISO code => array(array(grouping), positive_currency_string, negative_currency_string)) */
    public static $localeNumberFormat = array(
        %(numberData)s
    );

    /** @var array array(ISO code => array(decimal_char, group_char)) */
    public static $localCharacters = array(
        %(charData)s
    );
}
"""

    # Add some defaults
    data[1]['*'] = {'digits': 2, 'rounding': 0}
    data[2]['*'] = {'grouping': [], 'positive_currency_str': '$1 $2', 'negative_currency_str': '-$1 $2'}
    data[3]['*'] = {'decimal_char': '.', 'group_char': ',', }

    # Currency process
    phpCurrency = []
    for currency, idict in sorted(data[1].items()):
        phpCurrency.append("'%s' => array(%s, %s)" % (currency, idict['digits'], idict['rounding']))
    phpCurrency = ',\n        '.join(phpCurrency)

    # Number format process
    phpLocaleNumber = []
    for locale, idict in sorted(data[2].items()):
        phpLocaleNumber.append("'%s' => array(array(%s), '%s', '%s')" % (
            locale,
            ', '.join([str(x) for x in idict['grouping']]),
            idict['positive_currency_str'],
            idict['negative_currency_str']
        ))
    phpLocaleNumber = ',\n        '.join(phpLocaleNumber)

    # Symbol format process
    phpLocaleSymbol = []
    for locale, idict in sorted(data[3].items()):
        decimalChar = idict['decimal_char']
        if decimalChar == "'":
            decimalChar = "\\'"

        groupChar = idict['group_char']
        if groupChar == "'":
            groupChar = "\\'"

        phpLocaleSymbol.append("'%s' => array('%s', '%s')" % (locale, decimalChar, groupChar))
    phpLocaleSymbol = ',\n        '.join(phpLocaleSymbol)

    # Other file things
    if namespace is not None:
        namespace = "namespace %s;" % namespace
    else:
        namespace = ''

    f = codecs.open(outfile, 'w', 'utf-8')
    f.write(skeleton % {
		'namespace': namespace,
		'cldrVersion': data[0],
		'className': className,
		'currencyData': phpCurrency,
		'numberData': phpLocaleNumber,
		'charData': phpLocaleSymbol,
	})
    f.close()


def test():
    """Unit tests"""
    result = []

    out = extractNumericLocale('test', '¤ #,##,##0.00')
    expected = {
        'positive_currency_str': '$1 $2',
        'negative_currency_str': '-$1 $2',
        'grouping': [2, 3]
    }
    result.append(out)
    if out != expected:
        print("First test failed %s" % out)

    out = extractNumericLocale('test', '#,##0.00 ¤', '(#,##0.00) ¤')
    expected = {
        'positive_currency_str': '$2 $1',
        'negative_currency_str': '($2) $1',
        'grouping': [3]
    }
    result.append(out)
    if out != expected:
        print("Second test failed %s" % out)

    out = extractNumericLocale('test', '#0.00¤')
    expected = {
        'positive_currency_str': '$2$1',
        'negative_currency_str': '-$2$1',
        'grouping': []
    }
    result.append(out)
    if out != expected:
        print("Third test failed %s" % out)

    return result


if __name__ == "__main__":
    # === Extract options ===
    parser = OptionParser(usage="usage: %prog [options] <location of CLDR data>")
    parser.add_option("-o", "--outputFile", default='CldrData.php', help='Output file name')
    parser.add_option("-c", "--className", default='CldrData', help='Name of auto generated class')
    parser.add_option("-n", "--namespace", default=None, help='Namespace for generated class')
    parser.add_option("-t", "--test", default=False, help='Run tests')
    (options, args) = parser.parse_args()

    if options.test:
        test()
        exit()

    cldr_path = ''
    if len(args) != 1:
        parser.print_help()
        exit(1)
    else:
        cldr_path = args[0]

    # === Check to see if CLDR exists at path ===
    if cldr_path[-1] != os.path.sep:
        cldr_path += os.path.sep
    if not os.path.exists(cldr_path + 'main/en.xml'):
        print(
            "It appears that CLDR does not exist at the given path. Are you not pointing into the 'common' directory?"
        )
        exit(1)

    # Run it!
    outputData(parseCLDR(cldr_path), options.outputFile, options.className, options.namespace)
