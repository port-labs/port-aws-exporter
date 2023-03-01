import boto3

client = boto3.client('cloudcontrol')
cfn = boto3.client('cloudformation')
type_names = [t['TypeName'] for t in
              cfn.get_paginator('list_types').paginate(Type='RESOURCE', Visibility='PUBLIC').build_full_result()[
                  'TypeSummaries']]
supported = {}

for t in type_names:
    try:
        result = client.list_resources(TypeName=t)
        supported[t] = True
        print('type: %s success' % t)
    except client.exceptions.UnsupportedActionException:
        supported[t] = False
        print('type: %s unsupported' % t)
    except Exception as e:
        supported[t] = False
        print('type: %s error: %s' % (t, e))

print('supported %d of %d' % (sum(supported.values()), len(supported)))
