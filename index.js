const NATS = require('nats')
const { ApiPromise } = require('@polkadot/api')

const nc = NATS.connect();

const cache = {
      latest_block_height : 0,
      latest_block_hash: '',

      peer_url_list: []
};

function handle_new_header(header) {
      nc.publish('layer1.chain.newheader', `${header.number}.${header.hash}`)
}

async function main() {
      const api = await ApiPromise.create({
            types: {
                  Node: {
                        "key": "Bytes",
                        "amt": "u64"
                  },
                  Model: {
                        "account": "AccountId",
                        "payment": "u32",
                        "cid": "H256"
                  },
                  TeaId: "Vec<u8>",
                  PeerId: "Vec<u8>"
            }
      })

      // listen new block
      api.rpc.chain.subscribeNewHeads((header) => {
            //console.log(`chain is at #${header.number} has hash ${header.hash}`)
            cache.latest_block_hash = header.hash;
            cache.latest_block_height = header.number;
            handle_new_header(header)
      })

      nc.subscribe('layer1.async.*.>', async function (msg, reply, subject, sid) {
            console.log('Received a message: ', msg, reply, subject, sid)
            const subSections = subject.split('.')
            // console.log(subSections)
            if (subSections.length < 4) {
                  console.log('invalid subject')
                  return
            }
            const replyTo = subSections[2]
            const action = subSections[3]
            switch(action) {
                  case 'bootstrap':
                        nc.publish(reply, JSON.stringify(['tea-node1', 'tea-node2']))
                        break
                  case 'node_info':
                        const nodeInfo = await api.query.tea.nodes(msg)
                        nc.publish(reply, JSON.stringify(nodeInfo))
                        break
                  case 'latest_block':
                        nc.publish(reply, JSON.stringify(cache))
                        break
                  case 'get_block_hash':
                        const blockHash = await api.rpc.chain.getBlockHash(parseInt(msg))
                        nc.publish(reply, JSON.stringify(blockHash))
                        break

                  case 'put_peer_url':
                        // TODO put url to layer1
                        const list = {
                              list: put_peer_url(msg)
                        };
                        nc.publish(reply, JSON.stringify(list));
                        break;
                  default:
                        nc.publish(reply, JSON.stringify(['action_does_not_support']))
            }
      })
}

function put_peer_url(msg){
      console.log('receive peer url : ', msg);
      let n = cache.peer_url_list.find((x) => x === msg);
      if(!n){
            cache.peer_url_list.push(msg);
      }

      const rs = [];
      cache.peer_url_list.forEach((x) => {
            if(x !== msg){
                  rs.push(x);
            }
      })

      return rs;
}

main().catch((error) => {
      console.error(error)
      process.exit(-1)
})