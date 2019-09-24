paslon = 3

with open('data_paslon.txt', 'r') as f:
    for line in f.readlines():
        l = line.replace('\n', '').split(';')
#         print l
        _time = l[0].split(':')
        t = '20190417{}{}'.format(_time[0], _time[1])
        sql = """INSERT INTO pilpres_candidate_emotion_stat
                 VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
                 ON CONFLICT(minutes, candidate_id) DO UPDATE SET anticipation= excluded.anticipation, joy= excluded.joy, sadness=excluded.sadness, 
                 disgust=excluded.disgust, anger=excluded.anger, surprise=excluded.surprise, fear=excluded.fear, trust=excluded.trust;
              """.format(t, paslon, l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9])
        print sql