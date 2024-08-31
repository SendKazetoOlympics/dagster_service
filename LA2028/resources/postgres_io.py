from dagster import ConfigurableResource
import psycopg
from psycopg.rows import TupleRow

class PostgresResource(ConfigurableResource):

    host: str
    port: str
    dbname: str
    user: str
    password: str

    def get_client(self):
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )

    def selectVideoByDate(self, start_date: str, end_date: str) -> TupleRow:
        client = self.get_client()
        cursor = client.cursor()
        cursor.execute("SELECT * FROM videos WHERE cast(to_timestamp(start_time/1000) as date) BETWEEN %s AND %s ORDER BY start_time DESC", (start_date, end_date))
        return cursor.fetchall()
    
    def getTimeAnnotationsForVideo(self, video_id: str) -> TupleRow:
        client = self.get_client()
        cursor = client.cursor()
        cursor.execute("SELECT * FROM time_annotations WHERE video_id = %s", (video_id,))
        return cursor.fetchall()