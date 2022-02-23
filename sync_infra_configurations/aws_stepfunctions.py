import copy
import json

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.common_action as common_action
import sync_infra_configurations.aws as sic_aws

####################################################################################################
# StepFunctions
####################################################################################################

def execute_stepfunctions(action, src_data, session):
    stepfunctions_client = session.client("stepfunctions")
    return common_action.execute_elem_properties(action, src_data,
        executor_map = {
            "StateMachines": lambda action, src_data: execute_statemachines(action, src_data, session, stepfunctions_client),
        },
    )

####################################################################################################
# StepFunctions -> StateMachines
####################################################################################################

def execute_statemachines(action, src_data, session, stepfunctions_client):
    return common_action.execute_elem_items(action, src_data,
        list_fetcher = lambda: list_statemachines(session, stepfunctions_client),
        item_executor = lambda action, name, src_data: execute_statemachine(action, name, src_data, session, stepfunctions_client),
    )

def list_statemachines(session, stepfunctions_client):
    account_id = sic_aws.fetch_account_id(session)
    result = []
    res = stepfunctions_client.list_state_machines()
    while True:
        for elem in res['stateMachines']:
            name = elem["name"]
            arn = f"arn:aws:states:ap-northeast-1:{account_id}:stateMachine:{name}"
            if elem["stateMachineArn"] == arn:
                result.append(name)
        if not "nextToken" in res:
            break
        res = stepfunctions_client.list_state_machines(nextToken = res["nextToken"])
    return result

####################################################################################################
# StepFunctions -> StateMachines -> <machine_name>
####################################################################################################

def execute_statemachine(action, name, src_data, session, stepfunctions_client):
    return common_action.execute_elem_properties(action, src_data,
        describer = lambda: describe_statemachine(name, session, stepfunctions_client),
        updator = lambda src_data, curr_data: update_statemachine(name, src_data, curr_data, session, stepfunctions_client),
        help_generator = help_statemachine,
    )

def help_statemachine():
    return {
        "Definition": "the Amazon States Language definition of the state machine",
        "RoleArn": "ARN of the IAM role used when creating this state machine",
        "Type": "the type of the state machine (STANDARD or EXPRESS)",
        "LoggingConfiguration": "the LoggingConfiguration data type is used to set CloudWatch Logs options",
        "TracingConfiguration": "selects whether AWS X-Ray tracing is enabled",
    }

def describe_statemachine(name, session, stepfunctions_client):
    account_id = sic_aws.fetch_account_id(session)
    arn = f"arn:aws:states:ap-northeast-1:{account_id}:stateMachine:{name}"
    res = stepfunctions_client.describe_state_machine(stateMachineArn = arn)
    info = {}
    for key, value in res.items():
        key2 = key[0:1].title() + key[1:]
        info[key2] = value
    sic_lib.removeKey(info, "Name")
    sic_lib.removeKey(info, "StateMachineArn")
    sic_lib.removeKey(info, "Status")
    sic_lib.removeKey(info, "CreationDate")
    sic_lib.removeKey(info, "ResponseMetadata")
    info["Definition"] = definition_str_to_dict(info["Definition"])
    return info

def update_statemachine(name, src_data, curr_data, session, stepfunctions_client):
    if curr_data == None:
        # 新規作成
        sic_main.add_update_message(f"stepfunctions_client.create_state_machine(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = modify_data_for_put(src_data)
            stepfunctions_client.create_state_machine(name = name, **update_data)

    elif src_data == None:
        # 削除
        raise Exception("TODO")

    else:
        # 更新
        account_id = sic_aws.fetch_account_id(session)
        arn = f"arn:aws:states:ap-northeast-1:{account_id}:stateMachine:{name}"
        sic_main.add_update_message(f"stepfunctions_client.update_state_machine(stateMachineArn = {arn} ...)")
        if sic_main.put_confirmation_flag:
            update_data = modify_data_for_put(src_data)
            sic_lib.removeKey(update_data, "type")
            stepfunctions_client.update_state_machine(stateMachineArn = arn, **update_data)

def modify_data_for_put(update_data):
    update_data2 = {}
    for key, value in update_data.items():
        key2 = key[0:1].lower() + key[1:]
        update_data2[key2] = value
    update_data2["definition"] = definition_dict_to_str(update_data2["definition"])
    return update_data2

def definition_str_to_dict(definition: str):
    return json.loads(definition)

def definition_dict_to_str(definition: dict):
    return json.dumps(definition)

####################################################################################################
