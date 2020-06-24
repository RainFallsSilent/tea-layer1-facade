const assert = require('assert');
const { ApiPromise } = require('@polkadot/api');
const NATS = require('nats');
const proto = require('../src/proto');

const nc = NATS.connect();

function add_new_node() {
      nc.publish('layer1.async.replay.add_new_node', '0x02b40e313842e45574e0ca5b37cb0580cc3378ceb096b562a9828b2137b98f5f', 'layer1.test.result')
}

function update_peer_id() {
      nc.publish('layer1.async.replay.update_peer_id', '0x02b40e313842e45574e0ca5b37cb0580cc3378ceb096b562a9828b2137b98f5f__0x1fbb8d02600f4931fbed2e4f998d9e16d1a95e6d4586b5787310a95d2f8a6ed4_0xa555a7e72e9810dde46ca653d56956a2d6e88bb3896038f19674bd3b02d94d18', 'layer1.test.result')
}

function get_nodes() {
      nc.publish('layer1.async.replay.get_nodes', '', 'layer1.test.result')
}

function add_new_task() {
      const task = {
            teaId: Buffer.from('01', 'hex'),
            refNum: Buffer.from('abcdefg', 'hex'),
            rsaPub: Buffer.from('c7e016fad0796bb68594e49a6ef1942cf7e73497e69edb32d19ba2fab3696596', 'hex'),
            capCid: '111',
            manifestCid: '222',
            wasmCid: '333',
            modelCid: '444',
            dataCid: '555',
            payment: 1000,
      }
      const taskBuf = new proto.Protobuf('AddNewTaskRequest');
      taskBuf.payload({ task });

      const taskBufBase64 = Buffer.from(taskBuf.toBuffer()).toString('base64');

      // const protoMsg = Buffer.from(taskBuf, 'base64');
      // const newTaskBuf = new proto.Protobuf('AddNewTaskRequest');
      // const newTask = newTaskBuf.decode(protoMsg);
      // console.log('3', newTask);

      nc.publish('layer1.async.replay.add_new_task', taskBufBase64, 'layer1.test.result')
}

function complete_task() {
      nc.publish('layer1.async.replay.complete_task', '0x25484d12f935dbf24d116585edf4ce4936f3659390ea897b5081c66ac665f16e', 'layer1.test.result')
}

async function main() {
      // add_new_node()
      // update_peer_id()
      // get_nodes()
      add_new_task()
      // complete_task()
}

main().catch((error) => {
      console.error(error)
      process.exit(-1)
})