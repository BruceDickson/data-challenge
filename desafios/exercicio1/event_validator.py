import json
import boto3

_SQS_CLIENT = None
schema = open('schema.json')
schema = json.load(schema)
required_fields = schema['required']

type_map = {
        "string": str,
        "integer": int,
        "boolean": bool,
        "object": dict,
        "array": list,
        "double": float,
        "float": float
    }

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def handler(event):
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''
    try:
        field_types = get_fields_properties(schema)
        if check_valid_fields(event) and check_fields_quantity(event) and check_field_types(event, field_types):
            send_event_to_queue(event, "valid-events-queue")
    except Exception as e: print(e)


def check_valid_fields(event: dict) -> bool:
    '''
    Verify if exists fields in event that are not required (invalid)
    :param event: Event dict with data to be validated
    :return: True if any event field is out of required fields 
    '''
    for x in event:
        if x not in required_fields:
            raise ValueError(f'The field {x} is not a accepted field')
    return True

def check_fields_quantity(event: dict) -> bool:
    '''
    Compare the quantity of event fields and required fields
    :param event: Event dict with data to be validated
    :return: True if event fields and required fields has the same length
    '''
    if len(event) != len(required_fields):
        raise ValueError('Some required field(s) are missing')
    return True

def get_fields_properties(schema: dict) -> dict:
    '''
    Get json schema field types
    :param schema: Dict with the json schema
    :return: dict with columns and its types in python
    '''
    schema_prop = schema['properties']
    fields_prop = {}
    for field, value in schema_prop.items():
        fields_prop[field] = type_map[value['type']]
    return fields_prop

def check_field_types(event: dict, field_types: dict) -> bool:
    '''
    Verify if the types of event fields are the same of read json schema 
    :param event: Event dict with data to be validated
    :param field_types: Json schema field types
    :return: True if all event field types are compatible with json schema
    '''
    for field in event:
        if type(event[field]) != field_types[field]:
            raise ValueError(f'Incompatible type format for {field} field.')
    return True
    
