from datetime import datetime, timezone
import time


#Variables
sleeptime = 6


def get_cursor():
    import pymysql

    # Define Variables
    # Define Variables
    DBHost = 'mysqldb1.cmwln1uaaae0.ap-south-1.rds.amazonaws.com'
    DBUser = 'admin'
    DBPass = 'smartx123'
    DBName = 'waterfall'

    db = pymysql.connect(host=DBHost, user=DBUser, passwd=DBPass, db=DBName)
    crsr = db.cursor()

    return db, crsr



def get_local_cur():
    import pymysql

    # Define Variables
    # Define Variables
    DBHost = 'localhost'
    DBUser = 'root'
    DBPass = 'xxxx'
    DBName = 'waterfall'

    db = pymysql.connect(host=DBHost, user=DBUser, passwd=DBPass, db=DBName)
    crsr = db.cursor()

    return db, crsr




def check_time_diff(times, days):
    hours = minutes = seconds = ""
    if times >= 3600:
        hours = int(times / 3600)
        times = times - hours*3600
        if times >= 60:
            minutes = int(times/60)
            seconds = times - minutes*60
        else:
            seconds = times
    else:
        if times >= 60:
            minutes = int(times/60)
            seconds = times - minutes*60
        else:
            seconds = times
    hours = str(hours)
    minutes = str(minutes)
    seconds = str(seconds)
    if hours and minutes and seconds:
        duration = str(hours) +" Hours "+str(minutes)+" Minutes "
    elif minutes and seconds:
        duration = str(minutes)+" Minutes "
    elif seconds:
        duration = "Less than a minute"
    else:
        duration = "Unknown"
    if days > 0:
        duration = str(days)+" Days "+duration
    return duration


while True:
    time.sleep(sleeptime)
    db, crsr = get_cursor()
    db_local, crsr_local = get_local_cur()

    cmd = '''select * from temp_reading WHERE timestamp > (NOW() - INTERVAL 10 MINUTE) order by `timestamp` desc limit 1'''
    #cmd = '''select * from dummy_reading order by `timestamp` desc limit 1'''
    crsr.execute(cmd)
    cmd_get_last_record_in_cloud = '''select * from temp_reading order by id desc limit 1 ; '''
    data = crsr.fetchall()

    for d in data:
        dt = d[1].replace(tzinfo=timezone.utc).astimezone(tz=None).strftime("%d/%m/%Y %I:%M%p")
        ct = d[2]
        mx = d[3]
        mn = d[4]
        cmd = '''select * from toggle'''
        crsr.execute(cmd)
        check = crsr.fetchall()
        if check:
            start = check[0][1]
        if ct < mx and ct > mn:
            if check:
                # Logic to kill toggle, and add entry to alerts table
                status = check[0][0]
                start = check[0][1]
                peak = check[0][2]
                d = datetime.strptime(dt, "%d/%m/%Y %I:%M%p")
                s = datetime.strptime(start, "%d/%m/%Y %I:%M%p")
                t = d - s
                days = t.days
                times = t.seconds
                duration = check_time_diff(times,days)
                cmd = '''insert into alerts (`timestamp`,`status`,`start`,`end`,`duration`,`peak`) VALUES('%s','%s','%s','%s','%s','%s')''' % (dt,status,start,dt,duration,peak)
                crsr.execute(cmd)
                db.commit()
                cmd = '''TRUNCATE TABLE toggle'''
                crsr.execute(cmd)
                db.commit()
                print ("Temperature brought back to within allowed range")
            else:
                print ("Temperature is under control")
        elif ct >= mx:
            if check:
                #Logic to update toggle and alert table
                current = ct
                peak =  check[0][2]
                cmd = '''UPDATE toggle SET current = %s''' % (current,)
                crsr.execute(cmd)
                db.commit()
                print("Temperature Current value is " + str(current))
                if str(ct) > str(peak):
                    cmd = '''UPDATE toggle SET peak = %s''' % (current,)
                    crsr.execute(cmd)
                    db.commit()
                    print ("Temperature new peak value is "+str(current))
            else:
                # Logic to insert toggle and alert table
                cmd = '''insert into toggle (`status`,`time`,`peak`) VALUES('%s','%s','%s')''' % ('high',dt,ct)
                print (cmd)
                crsr.execute(cmd)
                db.commit()
                print("Toggle added with following details : "+'high'+","+str(dt)+","+str(ct))
        elif ct <= mn:
            if check:
                #Logic to update toggle and alert table
                if check:
                    # Logic to update toggle and alert table
                    current = ct
                    peak = check[0][2]
                    cmd = '''UPDATE toggle SET current = %s''' % (current,)
                    crsr.execute(cmd)
                    db.commit()
                    print("Temperature Current value is " + str(current))
                    if str(ct) < str(peak):
                        cmd = '''UPDATE toggle SET peak = %s''' % (current,)
                        crsr.execute(cmd)
                        db.commit()
                        print("Temperature new peak value is " + str(current))
            else:
                # Logic to insert toggle and alert table
                cmd = '''insert into toggle (`status`,`time`,`peak`) VALUES('%s','%s','%s')''' % ('low', dt, ct)
                print(cmd)
                crsr.execute(cmd)
                db.commit()
                print("Toggle added with following details : " + 'low' + "," + str(dt) + "," + str(ct))
    db.close()



