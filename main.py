import base64
import hashlib
import json
import time

from terra_sdk.client.lcd import LCDClient

CONTRACT_ADDRESS = 'terra12d90ss349sf5l8uxr0cxgly0n2e6e9hf3vxtn3'
chain_id = "bombay-12"
url = "https://bombay-lcd.terra.dev"
terra = LCDClient(chain_id=chain_id, url=url)

last_block_height = 7670812  # TODO: read from DB?
latest_block = int(terra.tendermint.block_info()['block']['header']['height'])

def process_transactions(transactions):
    for i, t in enumerate(transactions):
        print(f'Processing transaction {i}/{len(transactions)} in block {last_block_height}')
        hashed_tx = hashlib.sha256(base64.b64decode(t)).hexdigest()
        tx_info = terra.tx.tx_info(hashed_tx)
        rawlog = json.loads(tx_info.rawlog)
        events = rawlog[0]['events']

        is_contract = False
        for e in events:
            if e['type'] == 'execute_contract':
                contract_address = next(filter(lambda x: x['key'] == 'contract_address', e['attributes']))['value']
                if contract_address == CONTRACT_ADDRESS:
                    print(f'Contract address: {contract_address}')
                    is_contract = True

            elif e['type'] == 'from_contract':
                try:
                    _from = next(filter(lambda x: x['key'] == 'from', e['attributes']))['value']
                    _to = next(filter(lambda x: x['key'] == 'to', e['attributes']))['value']
                    data = next(filter(lambda x: x['key'] == 'data', e['attributes']))['value']
                    event = next(filter(lambda x: x['key'] == 'event', e['attributes']))['value']
                    print(f'From: {_from}, To: {_to}, Data: {data}, Event: {event}')
                except StopIteration:
                    pass

while True:
    if latest_block <= last_block_height:
        print(f'latest block: {latest_block} < {last_block_height}, waiting for new ones...')
        time.sleep(1)
        latest_block = int(terra.tendermint.block_info()['block']['header']['height'])
    else:
        print(f'latest block: {latest_block}, last processed block: {last_block_height}. Processing')
        transactions = terra.tendermint.block_info(last_block_height)['block']['data']['txs']
        try:
            process_transactions(transactions)
        except Exception as e:
            print(e)
        last_block_height += 1
