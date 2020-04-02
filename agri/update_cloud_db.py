#from datetime import datetime, timezone
import time
import pymysql
import sys

db_connection = {
    'remote': {
    'DBHost': 'mysqldb1.cmwln1uaaae0.ap-south-1.rds.amazonaws.com',
    'DBUser': 'admin',
    'DBPass': 'smartx123',
    'DBName': 'waterfall'
    },
    'local':{
     'DBHost': 'localhost',
     'DBUser': 'mounika',
     'DBPass': 'Mounika@123',
     'DBName': 'waterfall'
    }
}


def get_cursor(db_conn_type):
    """
    Create cursor for local and remote mysql server
    args: db_conn_type : (remote, local)
    :return: db, crsr
    """
    db = pymysql.connect(host=db_connection[db_conn_type]['DBHost'],
                         user=db_connection[db_conn_type]['DBUser'],
                         passwd=db_connection[db_conn_type]['DBPass'],
                         db=db_connection[db_conn_type]['DBName'])
    crsr = db.cursor()
    return db, crsr



def sync_local_to_remote_db():
    """
    Sync records in the local database to remote database;
    :return:
    """
    try:
        db_local, cur_local = get_cursor('local')
        db_remote, cur_remote = get_cursor('remote')

        # get last record id from remote database
        cmd_last_record = '''
        SELECT * FROM temp_reading
        ORDER BY id DESC 
        LIMIT 1 ;
        '''
        cur_remote.execute(cmd_last_record)
        last_record_id = cur_remote.fetchall()[0][0]
        print("Remote DB last record id: ", last_record_id)
        #update new records from local db to remote db

        cmd_new_local_records = '''
        SELECT * FROM temp_reading
        WHERE id > {last_id} 
        '''.format(last_id = last_record_id)

        cur_local.execute(cmd_new_local_records)
        local_records = cur_local.fetchall()

        if len(local_records) == 0:
            print('Local and Remote DBs are in sync..')
        for each_record in local_records:

            cmd_update = '''
            INSERT INTO temp_reading (id, timestamp, current_temp, high, low) 
            VALUES {values}
            '''.format(values=(each_record[0],
                               each_record[1].strftime("%Y-%m-%d, %H:%M:%S"),
                               float(each_record[2]),
                               float(each_record[3]),
                               float(each_record[4])))
            cur_remote.execute(cmd_update)
            db_remote.commit()
            print("Successfully inserted:", each_record)

    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == '__main__':
    sync_local_to_remote_db()