def decode_card_type(code):
    mapping = {
        'visa': [
            'BC000',
            'XV000',
            'BCVIY',
            'DE000',
            'XD000',
            'XVVIY',
            'PE000',
            'XE000',
            'VP001',
            'VPVIB',
            'VPVID',
            'VPVIR',
            'VPVIS',
            'VPVIX',
            'XP001',
            'XPVIB',
            'XPVID',
            'XPVIR',
            'XPVIS',
            'XPVIX',
        ],

        'mc': [
            'AC000',
            'ACMCW',
            'ACMNW',
            'XA000',
            'XAMCW',
            'XAMNW',
            'ACMCY',
            'DM000',
            'XAMCY',
            'XN000',
            'VP002',
            'VPMCB',
            'VPMCO',
            'VPMCP',
            'VPMCF',
            'VPMCX',
            'XP002',
            'XPMCB',
            'XPMCO',
            'XPMCP',
            'XPMCF',
            'XPMCX',
        ],

        'maestro': [
            'PM000',
            'XM000',
            'PMDOM',
            'XSDOM',
        ],

        'jcb': [
            'KF000',
        ],

        'laser': [
            'PT000',
        ],

        'other': [
            'AS000',
            'AX000',
            'BP000',
            'CM000',
            'CO000',
            'CY000',
            'DC000',
            'DL000',
            'EF000',
            'FS000',
            'GE000',
            'JC000',
            'LC000',
            'LE000',
            'LY000',
            'OD000',
            'PL000',
            'SB000',
            'SC000',
            'SE000',
            'SG000',
            'SH000',
            'SP000',
            'SY000',
            'TE000',
            'VC000',
            'VE000',
        ],
    }

    for type_name, codes in mapping.items():
        if code in codes:
            return type_name

    raise RuntimeError("Unknown card type code: {type}".format(type=code))
