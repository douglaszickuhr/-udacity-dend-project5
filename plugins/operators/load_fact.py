from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    insert_sql="""
    INSERT INTO
    {table} 
    {sql}
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        try:
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            self.log.info("Redshift hook defined")
        except AirflowException as e:
            self.log.error(e)
        
        # Inserting data from staging table into fact
        redshift.run(LoadFactOperator.insert_sql.format(table=self.table, sql=self.sql))
        
