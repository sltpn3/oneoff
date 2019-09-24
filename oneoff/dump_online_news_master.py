# s = 'mysqldump -h 192.168.150.160 -ubackend -prahasia123 printed_news {0}_{1}_{2} --insert-ignore --skip-add-drop-table |gzip -c > {0}_{1}_{2}.sql.gz'

s = 'tar -cvzf {0}-{1}-{2}.tgz {0}-{1}-{2}/'

# _years = ['2018']
_year = '2018'
# _months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
_month = '03'
_days = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
        '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
        '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
        '31']
langs = ['hc']

for _day in _days:
    print s.format(_year, _month, _day)
        