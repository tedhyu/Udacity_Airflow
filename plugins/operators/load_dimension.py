from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    copy_sql = """
        INSERT INTO {}
        {}
    """    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying Data to {}".format(self.table))
        formatted_sql = LoadDimensionOperator.copy_sql.format(
            self.table,
            self.sql)
        redshift.run(formatted_sql)
