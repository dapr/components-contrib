#!/usr/bin/env python3
# Copyright 2021 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import ipaddress
import json
import os
import subprocess
import sys
import urllib.request

def parseArgs():
    abscwd = os.path.abspath(os.getcwd())
    arg_parser = argparse.ArgumentParser(description='Generates the IP ranges based on CIDRs for GitHub Actions from meta API.')
    arg_parser.add_argument('--outpath', type=str, default=abscwd, help='Optional. Full path to write the JSON output to.')
    arg_parser.add_argument('--sqlserver', type=str, help='Name of the Azure SQL server to update firewall rules of. Required for deployment.')
    arg_parser.add_argument('--resource-group', type=str, help='Resouce group containing the target Azure SQL server. Required for deployment.')
    arg_parser.add_argument('--no-deployment', action='store_true', help='Specify this flag to generate the ARM template without deploying it.')
    args = arg_parser.parse_args()

    if not args.no_deployment:
        is_missing_args = False
        if not args.sqlserver:
            print('ERROR: the following argument is required: --sqlserver')
            is_missing_args = True
        if not args.resource_group:
            print('ERROR: the following argument is required: --resource-group')
            is_missing_args = True
        if is_missing_args:
            arg_parser.print_help()
            sys.exit(-1)

    print('Arguments parsed: {}'.format(args))
    return args

def getResponse(url):
    operUrl = urllib.request.urlopen(url)
    if(operUrl.getcode()==200):
        data = operUrl.read()
        jsonData = json.loads(data)
    else:
        print('ERROR: failed to receive data', operUrl.getcode())
    return jsonData

def writeAllowedIPRangesJSON(outpath):
    url = 'https://api.github.com/meta'
    jsonData = getResponse(url)

    ipRanges = []
    prevStart = ''
    prevEnd = ''

    # Iterate the public IP CIDRs used to run GitHub Actions, and convert them
    # into IP ranges for test SQL server firewall access.
    for cidr in jsonData['actions']:
        net = ipaddress.ip_network(cidr)
        # SQL server firewall only supports up to 128 firewall rules.
        # As a first cut, exclude all IPv6 addresses.
        if net.version == 4:
            start = net[0]
            end = net[-1]
            # print(f'{cidr} --> [{start}, {end}]')

            if prevStart == '':
                prevStart = start
            if prevEnd == '':
                prevEnd = end
            elif prevEnd + 65536 > start:
                # If the current IP range is within the granularity of a /16
                # subnet mask to the previous range, coalesce them into one.
                # This is necessary to get the number of rules down to ~100.
                prevEnd = end
            else:
                ipRange = [str(prevStart), str(prevEnd)]
                ipRanges.append(ipRange)
                prevStart = start
                prevEnd = end

    if prevStart != '' and prevEnd != '':
        ipRange = [str(prevStart), str(prevEnd)]
        ipRanges.append(ipRange)

    with open(outpath, 'w') as outfile:
        json.dump(ipRanges, outfile)

def main():
    args = parseArgs()

    # Get the GitHub IP Ranges to use as firewall allow-rules from the GitHub meta API
    ipRangesFileName = os.path.join(args.outpath, 'github-ipranges.json')
    writeAllowedIPRangesJSON(ipRangesFileName)
    print(f'INFO: GitHub Actions public IP range rules written {ipRangesFileName}')

    # Generate the ARM template from bicep to update Azure SQL server firewall rules
    subprocess.call(['az', 'bicep', 'install'])
    firewallTemplateName = os.path.join(args.outpath, 'update-sql-firewall-rules.json')
    subprocess.call(['az', 'bicep', 'build', '--file', 'conf-test-azure-sqlserver-firewall.bicep', '--outfile', firewallTemplateName])
    print(f'INFO: ARM template to update SQL Server firewall rules written to {firewallTemplateName}')

    # Update the Azure SQL server firewall rules
    if args.no_deployment:
        print(f'INFO: --no-deployment specified, skipping update of SQL server {firewallTemplateName}')
    else:
        subprocess.call(['az', 'deployment', 'group', 'create', '--name', 'UpdateSQLFirewallRules', '--template-file', firewallTemplateName, '--resource-group', args.resource_group, '--parameters', f'sqlServerName={args.sqlserver}', '--parameters', f'ipRanges=@{ipRangesFileName}'])

    sys.exit(0)

if __name__ == '__main__':
    main()
