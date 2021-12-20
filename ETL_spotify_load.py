import psycopg2
from requests.models import CONTENT_CHUNK_SIZE
import ETL_spotify_transform as transform

# ================= PostgresSql Credentials===============
hostname = 'localhost'
database = 'spotify'
username = 'postgres'
pwd = '365pass'
port_id = 5432
song_df = transform.song_df
# ================= Loading stage !=======================


def load_to_db():
    conn = None
    try:
        with psycopg2.connect(
                host=hostname,
                dbname=database,
                user=username,
                password=pwd,
                port=port_id) as conn:
            with conn.cursor() as cur:
                cur.execute('drop table if exists songs')
                CREATE_Script = '''
            create table if not exists songs(
                song_name varchar(90) not null,
                artist_name varchar(90)not null,
                played_at varchar(200) primary key,
                timestamp varchar(90) not null
            )
            '''
                cur.execute(CREATE_Script)
                if transform.check_if_valid_data(song_df):
                    song_df.to_csv('played_tracks.csv', index=False)
                    with open('played_tracks.csv') as my_file:
                        COPY_Script = 'COPY songs FROM STDIN WITH CSV HEADER DELIMITER AS \',\''
                        cur.copy_expert(sql=COPY_Script, file=my_file)
                        print("file copied to db !")

                else:
                    print("Data not valid !")

    except Exception as e:
        print(e)

    finally:
        if conn != None:
            conn.close()
