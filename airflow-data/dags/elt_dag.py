from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import base64

# padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# base onde arquivos são armazenados
base_dir = Variable.get('base_dir', default_var='/mnt/c/Users/tacia/Documents/Desafio_7_Airflow/airflow_tooltorial/airflow-data/dags')

# caminho do banco de dados - SQLite
db_path = Variable.get('db_path', default_var='/mnt/c/Users/tacia/Documents/Desafio_7_Airflow/airflow_tooltorial/data/Northwind_small.sqlite')

def extract_orders():
    """Extrai dados da tabela 'Order' e salva em CSV usando Pandas."""
    logging.info("Iniciando extração de pedidos...")
    try:
        # 'Order' usando Pandas
        orders_df = pd.read_sql('SELECT * FROM "Order"', f'sqlite:///{db_path}')
        output_file = os.path.join(base_dir, 'output_orders.csv')
        orders_df.to_csv(output_file, index=False)  # CSV

        logging.info(f"Extração concluída. Total de pedidos extraídos: {len(orders_df)}. Arquivo salvo em {output_file}")
    except Exception as e:
        logging.error(f"Erro ao acessar o banco de dados: {e}")

def process_orders(ship_city='Rio de Janeiro'):
    """Realiza o JOIN e calcula a soma de 'Quantity' para um ShipCity específico usando Pandas."""
    logging.info("Iniciando processamento de pedidos...")
    try:
        # lê as tabelas 
        orders_df = pd.read_sql('SELECT * FROM "Order"', f'sqlite:///{db_path}')
        order_details_df = pd.read_sql('SELECT * FROM "OrderDetail"', f'sqlite:///{db_path}')

        # JOIN - calcula a soma da quantidade
        merged_df = pd.merge(order_details_df, orders_df, left_on='OrderID', right_on='ID')
        total_quantity = merged_df[merged_df['ShipCity'] == ship_city]['Quantity'].sum()

        output_file = os.path.join(base_dir, 'count.txt')
        with open(output_file, 'w') as f:
            f.write(str(total_quantity))  # count.txt

        logging.info(f"Processamento concluído. Resultado salvo em {output_file}")
    except Exception as e:
        logging.error(f"Erro ao processar os pedidos: {e}")

def export_final_answer():
    """Gera o arquivo final_output.txt com a soma em base64."""
    logging.info("Iniciando geração do arquivo final_output.txt...")
    try:
        count_file = os.path.join(base_dir, 'count.txt')
        with open(count_file) as f:
            count = f.read().strip()

        my_email = Variable.get('my_email', default_var='taciana.vasconcelos@indicium.tech')
        message = my_email + count

        output_file = os.path.join(base_dir, 'final_output.txt')
        with open(output_file, 'w') as f:
            f.write(base64.b64encode(message.encode('ascii')).decode('ascii'))

        logging.info(f"Arquivo final_output.txt gerado com sucesso em {output_file}")
    except Exception as e:
        logging.error(f"Erro ao gerar o arquivo final: {e}")

# DAG - desafio
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    task_extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    task_process_orders = PythonOperator(
        task_id='process_orders',
        python_callable=process_orders,
    )

    task_export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
    )

    task_extract_orders >> task_process_orders >> task_export_final_output
