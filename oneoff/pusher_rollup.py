from library.eBeanstalk import Pusher
import MySQLdb


conn = MySQLdb.connect('192.168.150.22', 'backend', 'rahasia123', 'ipa_main')
cursor = conn.cursor(MySQLdb.cursors.DictCursor)
pusher = Pusher('demography_twitter_topic_rollup', '192.168.150.21')

sql = 'SELECT t_id FROM topic WHERE t_status = 1'
cursor.execute(sql)
rows = cursor.fetchall()
cursor.close()
conn.close()

topics = []

for row in rows:
    topics.append(row['t_id'])
    
# topics = [5494]

months = ['01', '02']
years = ['2019']
days = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

for year in reversed(years):
    for month in reversed(months):
        for day in reversed(days):
            for topic in topics:
                job = '{}|{}{}{}'.format(topic, year, month, day)
                pusher.setJob(job)
                print job