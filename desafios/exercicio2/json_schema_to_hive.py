import json
from venv import create

_ATHENA_CLIENT = None
template_hql = "hql/create_table_template.hql"
table_name = 'exercicio2' #Talvez trazer essa informação do jsonschema
schema = open('schema.json')
schema = json.load(schema)

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''
    template = load_template(template_hql)
    fields_prop = get_fields_properties(schema)
    columns = create_columns_names_type(fields_prop)
    query = build_query(template, table_name, columns)

    create_hive_table_with_athena(query)

def load_template(path: str) -> str:
    '''
    Read create table template
    :param path: Path of template
    :return: string with create table template
    '''
    with open(path, 'r') as template:
        return template.read()

def get_fields_properties(schema: dict) -> dict:
    '''
    Get json schema field types
    :param schema: Dict with the json schema
    :return: dict with columns and its types
    '''
    schema_prop = schema['properties']
    fields_prop = {}
    for field, value in schema_prop.items():
        
        fields_prop[field] = value['type']
    return fields_prop

def create_columns_names_type(fields: dict) -> str:
    '''
    Create prepared columns and types to the query
    :param fields: Json schema field types
    :return: string with prepared columns and types
    '''
    columns = [f + ' ' + t for f, t in fields.items()]
    formatted_columns = ', '.join(columns)
    return formatted_columns 

def build_query(template: str, table_name: str, columns: str) -> str:
    '''
    Build create table query
    :param template: Template of create query
    :param table_name: Name of the table to be created
    :param columns: Prepared columns and types
    :return: string with complete create table query
    '''
    return f'{template}'.format(table_name=table_name, columns=columns)