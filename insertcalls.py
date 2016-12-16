import json
import boto3
from datetime import datetime
from collections import defaultdict
import uuid
import time
from faker import Factory
import re
from itertools import cycle
import time
import random
from iso3166 import countries


ISO3166 = {
    'AF': 'AFGHANISTAN',
    'AX': 'ALAND ISLANDS',
    'AL': 'ALBANIA',
    'DZ': 'ALGERIA',
    'AS': 'AMERICAN SAMOA',
    'AD': 'ANDORRA',
    'AO': 'ANGOLA',
    'AI': 'ANGUILLA',
    'AQ': 'ANTARCTICA',
    'AG': 'ANTIGUA AND BARBUDA',
    'AR': 'ARGENTINA',
    'AM': 'ARMENIA',
    'AW': 'ARUBA',
    'AU': 'AUSTRALIA',
    'AT': 'AUSTRIA',
    'AZ': 'AZERBAIJAN',
    'BS': 'BAHAMAS',
    'BH': 'BAHRAIN',
    'BD': 'BANGLADESH',
    'BB': 'BARBADOS',
    'BY': 'BELARUS',
    'BE': 'BELGIUM',
    'BZ': 'BELIZE',
    'BJ': 'BENIN',
    'BM': 'BERMUDA',
    'BT': 'BHUTAN',
    'BO': 'BOLIVIA, PLURINATIONAL STATE OF',
    'BQ': 'BONAIRE, SINT EUSTATIUS AND SABA',
    'BA': 'BOSNIA AND HERZEGOVINA',
    'BW': 'BOTSWANA',
    'BV': 'BOUVET ISLAND',
    'BR': 'BRAZIL',
    'IO': 'BRITISH INDIAN OCEAN TERRITORY',
    'BN': 'BRUNEI DARUSSALAM',
    'BG': 'BULGARIA',
    'BF': 'BURKINA FASO',
    'BI': 'BURUNDI',
    'KH': 'CAMBODIA',
    'CM': 'CAMEROON',
    'CA': 'CANADA',
    'CV': 'CAPE VERDE',
    'KY': 'CAYMAN ISLANDS',
    'CF': 'CENTRAL AFRICAN REPUBLIC',
    'TD': 'CHAD',
    'CL': 'CHILE',
    'CN': 'CHINA',
    'CX': 'CHRISTMAS ISLAND',
    'CC': 'COCOS (KEELING) ISLANDS',
    'CO': 'COLOMBIA',
    'KM': 'COMOROS',
    'CG': 'CONGO',
    'CD': 'CONGO, THE DEMOCRATIC REPUBLIC OF THE',
    'CK': 'COOK ISLANDS',
    'CR': 'COSTA RICA',
    'CI': 'CTEDIVOIRE',
    'HR': 'CROATIA',
    'CU': 'CUBA',
    'CW': 'CURAAO',
    'CY': 'CYPRUS',
    'CZ': 'CZECH REPUBLIC',
    'DK': 'DENMARK',
    'DJ': 'DJIBOUTI',
    'DM': 'DOMINICA',
    'DO': 'DOMINICAN REPUBLIC',
    'EC': 'ECUADOR',
    'EG': 'EGYPT',
    'SV': 'EL SALVADOR',
    'GQ': 'EQUATORIAL GUINEA',
    'ER': 'ERITREA',
    'EE': 'ESTONIA',
    'ET': 'ETHIOPIA',
    'FK': 'FALKLAND ISLANDS (MALVINAS)',
    'FO': 'FAROE ISLANDS',
    'FJ': 'FIJI',
    'FI': 'FINLAND',
    'FR': 'FRANCE',
    'GF': 'FRENCH GUIANA',
    'PF': 'FRENCH POLYNESIA',
    'TF': 'FRENCH SOUTHERN TERRITORIES',
    'GA': 'GABON',
    'GM': 'GAMBIA',
    'GE': 'GEORGIA',
    'DE': 'GERMANY',
    'GH': 'GHANA',
    'GI': 'GIBRALTAR',
    'GR': 'GREECE',
    'GL': 'GREENLAND',
    'GD': 'GRENADA',
    'GP': 'GUADELOUPE',
    'GU': 'GUAM',
    'GT': 'GUATEMALA',
    'GG': 'GUERNSEY',
    'GN': 'GUINEA',
    'GW': 'GUINEA-BISSAU',
    'GY': 'GUYANA',
    'HT': 'HAITI',
    'HM': 'HEARD ISLAND AND MCDONALD ISLANDS',
    'VA': 'HOLY SEE (VATICAN CITY STATE)',
    'HN': 'HONDURAS',
    'HK': 'HONG KONG',
    'HU': 'HUNGARY',
    'IS': 'ICELAND',
    'IN': 'INDIA',
    'ID': 'INDONESIA',
    'IR': 'IRAN, ISLAMIC REPUBLIC OF',
    'IQ': 'IRAQ',
    'IE': 'IRELAND',
    'IM': 'ISLE OF MAN',
    'IL': 'ISRAEL',
    'IT': 'ITALY',
    'JM': 'JAMAICA',
    'JP': 'JAPAN',
    'JE': 'JERSEY',
    'JO': 'JORDAN',
    'KZ': 'KAZAKHSTAN',
    'KE': 'KENYA',
    'KI': 'KIRIBATI',
    'KP': 'KOREA, DEMOCRATIC PEOPLE\'S REPUBLIC OF',
    'KR': 'KOREA, REPUBLIC OF',
    'KW': 'KUWAIT',
    'KG': 'KYRGYZSTAN',
    'LA': 'LAO PEOPLE\'S DEMOCRATIC REPUBLIC',
    'LV': 'LATVIA',
    'LB': 'LEBANON',
    'LS': 'LESOTHO',
    'LR': 'LIBERIA',
    'LY': 'LIBYAN ARAB JAMAHIRIYA',
    'LI': 'LIECHTENSTEIN',
    'LT': 'LITHUANIA',
    'LU': 'LUXEMBOURG',
    'MO': 'MACAO',
    'MK': 'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF',
    'MG': 'MADAGASCAR',
    'MW': 'MALAWI',
    'MY': 'MALAYSIA',
    'MV': 'MALDIVES',
    'ML': 'MALI',
    'MT': 'MALTA',
    'MH': 'MARSHALL ISLANDS',
    'MQ': 'MARTINIQUE',
    'MR': 'MAURITANIA',
    'MU': 'MAURITIUS',
    'YT': 'MAYOTTE',
    'MX': 'MEXICO',
    'FM': 'MICRONESIA, FEDERATED STATES OF',
    'MD': 'MOLDOVA, REPUBLIC OF',
    'MC': 'MONACO',
    'MN': 'MONGOLIA',
    'ME': 'MONTENEGRO',
    'MS': 'MONTSERRAT',
    'MA': 'MOROCCO',
    'MZ': 'MOZAMBIQUE',
    'MM': 'MYANMAR',
    'NA': 'NAMIBIA',
    'NR': 'NAURU',
    'NP': 'NEPAL',
    'NL': 'NETHERLANDS',
    'NC': 'NEW CALEDONIA',
    'NZ': 'NEW ZEALAND',
    'NI': 'NICARAGUA',
    'NE': 'NIGER',
    'NG': 'NIGERIA',
    'NU': 'NIUE',
    'NF': 'NORFOLK ISLAND',
    'MP': 'NORTHERN MARIANA ISLANDS',
    'NO': 'NORWAY',
    'OM': 'OMAN',
    'PK': 'PAKISTAN',
    'PW': 'PALAU',
    'PS': 'PALESTINIAN TERRITORY, OCCUPIED',
    'PA': 'PANAMA',
    'PG': 'PAPUA NEW GUINEA',
    'PY': 'PARAGUAY',
    'PE': 'PERU',
    'PH': 'PHILIPPINES',
    'PN': 'PITCAIRN',
    'PL': 'POLAND',
    'PT': 'PORTUGAL',
    'PR': 'PUERTO RICO',
    'QA': 'QATAR',
    'RE': 'XXXXX',
    'RO': 'ROMANIA',
    'RU': 'RUSSIAN FEDERATION',
    'RW': 'RWANDA',
    'SH': 'SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA',
    'KN': 'SAINT KITTS AND NEVIS',
    'LC': 'SAINT LUCIA',
    'MF': 'SAINT MARTIN (FRENCH PART)',
    'PM': 'SAINT PIERRE AND MIQUELON',
    'VC': 'SAINT VINCENT AND THE GRENADINES',
    'WS': 'SAMOA',
    'SM': 'SAN MARINO',
    'ST': 'SAO TOME AND PRINCIPE',
    'SA': 'SAUDI ARABIA',
    'SN': 'SENEGAL',
    'RS': 'SERBIA',
    'SC': 'SEYCHELLES',
    'SL': 'SIERRA LEONE',
    'SG': 'SINGAPORE',
    'SX': 'SINT MAARTEN (DUTCH PART)',
    'SK': 'SLOVAKIA',
    'SI': 'SLOVENIA',
    'SB': 'SOLOMON ISLANDS',
    'SO': 'SOMALIA',
    'ZA': 'SOUTH AFRICA',
    'GS': 'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS',
    'SS': 'SOUTH SUDAN',
    'ES': 'SPAIN',
    'LK': 'SRI LANKA',
    'SD': 'SUDAN',
    'SR': 'SURINAME',
    'SJ': 'SVALBARD AND JAN MAYEN',
    'SZ': 'SWAZILAND',
    'SE': 'SWEDEN',
    'CH': 'SWITZERLAND',
    'SY': 'SYRIAN ARAB REPUBLIC',
    'TW': 'TAIWAN, PROVINCE OF CHINA',
    'TJ': 'TAJIKISTAN',
    'TZ': 'TANZANIA, UNITED REPUBLIC OF',
    'TH': 'THAILAND',
    'TL': 'TIMOR-LESTE',
    'TG': 'TOGO',
    'TK': 'TOKELAU',
    'TO': 'TONGA',
    'TT': 'TRINIDAD AND TOBAGO',
    'TN': 'TUNISIA',
    'TR': 'TURKEY',
    'TM': 'TURKMENISTAN',
    'TC': 'TURKS AND CAICOS ISLANDS',
    'TV': 'TUVALU',
    'UG': 'UGANDA',
    'UA': 'UKRAINE',
    'AE': 'UNITED ARAB EMIRATES',
    'GB': 'UNITED KINGDOM',
    'US': 'UNITED STATES',
    'UM': 'UNITED STATES MINOR OUTLYING ISLANDS',
    'UY': 'URUGUAY',
    'UZ': 'UZBEKISTAN',
    'VU': 'VANUATU',
    'VE': 'VENEZUELA, BOLIVARIAN REPUBLIC OF',
    'VN': 'VIET NAM',
    'VG': 'VIRGIN ISLANDS, BRITISH',
    'VI': 'VIRGIN ISLANDS, U.S.',
    'WF': 'WALLIS AND FUTUNA',
    'EH': 'WESTERN SAHARA',
    'YE': 'YEMEN',
    'ZM': 'ZAMBIA',
    'ZW': 'ZIMBABWE',
    }

faker = Factory.create()


dynamodb=boto3.resource(service_name='dynamodb', region_name='eu-west-1')

table = dynamodb.Table("callstab")

itemcount = 0



print ('Insert start time :', time.strftime("%H:%M:%S"))


calltype = ["mobile", "nongeo", "inbound", 'nat', "premium","inter", "loc"]
calldirecttion = ["INCOMIN", "OUTGOING"]

calltypeidx = cycle(range(7))
calldirecttionidx = cycle(range(2))

# import pdb; pdb.set_trace()

# location = countries.get(re.sub(r'([^\s\w]|_)+', '', faker.country()).replace(' ', '_')).alpha2
# loc = random.choice(ISO3166.keys())
# import pdb; pdb.set_trace()
# location2 = location.alpha2



for i in xrange(10):
    acccount = 0
    accountid = "ACC-123" + str(i)
    for a in xrange(10): 
        if a%2==0:
            trunkid="TRK-X456-" + str(a) + "-" + accountid
        else:
            trunkid="TRK-Y456-" + str(a) + "-" + accountid   

        source = "+4414822425-" + trunkid
        trkcount =0
        numcount =0
        
        for c in xrange(500):
            itemcount = itemcount + 1
            callid = str(uuid.uuid4())
            response = table.put_item(
               Item={
                    'callid': callid,
                    'accountid': accountid,
                    'trunkid':trunkid,
                    'source': source,
                    'location': random.choice(ISO3166.keys()),
                    'calltype': calltype[calltypeidx.next()],
                    'calldirection': calldirecttion[calldirecttionidx.next()],
                    'duration': random.randint(30, 600),
                    'calldate': int(datetime.now().strftime("%Y%m%d%H%M%S"))

                    }
                )        
            acccount += 1
            trkcount += 1
            numcount += 1
        time.sleep( 5)
        print("trunk ", trunkid, " : ", trkcount )
        print("number ", source, " : ", numcount )
    print("account ", accountid, " : ", acccount )
print ('Insert end time :', time.strftime("%H:%M:%S"))

print ('Insert item count:', itemcount)
