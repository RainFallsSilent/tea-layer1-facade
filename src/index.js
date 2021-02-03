/* eslint-disable camelcase */
const NATS = require('nats')
const { ApiPromise, WsProvider, Keyring } = require('@polkadot/api')
const { cryptoWaitReady } = require('@polkadot/util-crypto')
const _ = require('lodash')
const proto = require('./proto')
const toHex = require('to-hex')
const types = require('./types')
const rpc = require('./rpc')
const BN = require('bn.js')

const natsUrl = process.env.NATS_URL || '127.0.0.1:4222'
const nc = NATS.connect(natsUrl, {
  waitOnFirstConnect: true
})

const cache = {
  latest_block_height: 0,
  latest_block_hash: '',

  peer_url_list: []
}

const yi = new BN('100000000', 10)
const million = new BN('10000000', 10)
const unit = yi.mul(million)

function sleep (time) {
  return new Promise((resolve) => setTimeout(resolve, time))
}

async function main () {
  const wsProvider = new WsProvider(process.env.FACADE_WSURL || 'ws://127.0.0.1:9944')
  let api = {}
  while (true) {
    try {
      api = await ApiPromise.create({
        provider: wsProvider,
        types,
        rpc
      })
      break
    } catch (e) {
      console.log('Try reconnect ...')
      await sleep(2000)
    }
  }

  await cryptoWaitReady()

  const keyring = new Keyring({ type: 'sr25519' })

  const account = process.env.FACADE_ACCOUNT_URI || '//Alice'
  const ac = keyring.addFromUri(account)

  console.log(`facade use account: ${ac.address}`)

  // listen new block
  api.rpc.chain.subscribeNewHeads((header) => {
    // console.log(`chain is at #${header.number} has hash ${header.hash}`)
    cache.latest_block_hash = header.hash
    cache.latest_block_height = header.number
    handle_new_header(header)
  })

  // Subscribe to system events via storage
  api.query.system.events((events) => {
    handle_events(events)
  })

  nc.subscribe('layer1.async.*.>', async function (msg, reply, subject, sid) {
    // console.log('Received a message: ', msg, reply, subject, sid)
    const subSections = subject.split('.')
    // console.log(subSections)
    if (subSections.length < 4) {
      console.log('invalid subject')
      return
    }
    // const replyTo = subSections[2]
    const action = subSections[3]

    switch (action) {
      case 'latest_block':
        nc.publish(reply, JSON.stringify(cache))
        break
      case 'get_block_hash': {
        const blockHash = await api.rpc.chain.getBlockHash(parseInt(msg))
        nc.publish(reply, JSON.stringify(blockHash))
        break
      }
      case 'add_new_node': {
        const newRequestBuf = new proto.DelegateProtobuf('AddNewNodeRequest')
        const newNodeRequest = newRequestBuf.decode(Buffer.from(msg, 'base64'))

        const teaId = toHex(newNodeRequest.teaId, { addPrefix: true })

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.tea.addNewNode(teaId)
          .signAndSend(ac, {nonce: nonce}, ({ events = [], status }) => {
            if (status.isInBlock) {
              console.log('Add new node with teaId ' + teaId)
              nc.publish(reply, JSON.stringify({ status, teaId }))
            } else {
              console.log('Status of transfer: ' + status.type)
            }

            events.forEach(({ phase, event: { data, method, section } }) => {
              console.log(phase.toString() + ' : ' + section + '.' + method + ' ' + data.toString())
            })
          })
        console.log('send add_new_node tx')
        break
      }
      case 'commit_ra_result': {
        const newRequestBuf = new proto.RAProtobuf('CommitRaResultRequest')
        const newRequest = newRequestBuf.decode(Buffer.from(msg, 'base64'))

        const teaId = toHex(newRequest.teaId, { addPrefix: true })
        const targetTeaId = toHex(newRequest.targetTeaId, { addPrefix: true })
        const isPass = newRequest.isPass
        const signature = toHex(newRequest.signature, { addPrefix: true })

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.tea.remoteAttestation(teaId, targetTeaId, isPass, signature)
          .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
            if (status.isInBlock) {
              console.log('Commit remote attestation result, teaId:' + teaId)
              nc.publish(reply, JSON.stringify({ status, teaId }))
            } else {
              console.log('Status of transfer: ' + status.type)
            }

            events.forEach(({ phase, event: { data, method, section } }) => {
              console.log(phase.toString() + ' : ' + section + '.' + method + ' ' + data.toString())
            })
          })
        console.log('send commit_ra_result tx')
        break
      }
      case 'update_node_profile': {
        const uProtoMsg = Buffer.from(msg, 'base64')
        const updateProfileBuf = new proto.RAProtobuf('TeaNodeUpdateProfileRequest')
        const updateProfile = updateProfileBuf.decode(uProtoMsg)
        // console.log(updateProfile);

        var teaId = toHex(updateProfile.teaId, { addPrefix: true })
        var ephemeralPublicKey = toHex(updateProfile.ephemeralPublicKey, { addPrefix: true })
        const profileCid = toHex(Buffer.from(updateProfile.profileCid), { addPrefix: true })
        const peerId = toHex(Buffer.from(updateProfile.peerId), { addPrefix: true })
        const publicUrls = []
        updateProfile.publicUrls.forEach((url, i) => {
          publicUrls.push(toHex(Buffer.from(url), { addPrefix: true }))
        })
        const signature = toHex(Buffer.from(updateProfile.signature), { addPrefix: true })

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.tea.updateNodeProfile(teaId, ephemeralPublicKey, profileCid, publicUrls, peerId, signature)
          .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
            if (status.isInBlock) {
              console.log('Update node profile with teaId ' + teaId)
            } else {
              console.log('Status of transfer: ' + status.type)
            }

            events.forEach(({ phase, event: { data, method, section } }) => {
              console.log(phase.toString() + ' : ' + section + '.' + method + ' ' + data.toString())
            })
          })
        console.log('send update_peer_id tx')
        break
      }
      case 'get_nodes': {
        const nodes = await api.query.tea.nodes.entries()
        const teaNodes = nodes.map((n) => {
          return n[1]
        })
        nc.publish(reply, JSON.stringify(teaNodes))
        break
      }
      case 'lookup_node_profile': {
        const ephemeralId = toHex(Buffer.from(msg, 'base64'), { addPrefix: true })
        const teaId = await api.query.tea.ephemeralIds(ephemeralId)
        if (teaId.isNone) {
          console.log('No such ephemeral id found', ephemeralId)
          nc.publish(reply, 'no_such_ephemeral_id_found')
          break
        }
        const nodeObj = await api.query.tea.nodes(teaId.unwrap())
        if (nodeObj.isNone) {
          console.log('No such node found', teaId.toString())
          nc.publish(reply, 'no_such_node_found')
          break
        }
        const node = nodeObj.toJSON()

        if (node == null) {
          nc.publish(reply, 'node_is_not_exist')
          break
        }
        const urls = []
        if (node.urls) {
          node.urls.forEach((url, i) => {
            urls.push(Buffer.from(url.slice(2), 'hex').toString())
          })
        }
        const raNodes = []
        if (node.raNodes) {
          node.raNodes.forEach((raNode, i) => {
            raNodes.push({
              teaId: Buffer.from(raNode[0], 'hex'),
              isPass: Boolean(raNode[1])
            })
          })
        }
        const nodeProfile = {
          teaId: Buffer.from(node.teaId.slice(2), 'hex'),
          ephemeralPublicKey: Buffer.from(node.ephemeralId.slice(2), 'hex'),
          profileCid: Buffer.from(node.profileCid.slice(2), 'hex').toString(),
          peerId: Buffer.from(node.peerId.slice(2), 'hex').toString(),
          publicUrls: urls,
          raNodes,
          status: node.status.toString()
        }
        console.log('Lookup node profile:', JSON.stringify(nodeProfile))
        const nodeBuf = new proto.RAProtobuf('NodeProfile')
        nodeBuf.payload(nodeProfile)
        const nodeBase64 = Buffer.from(nodeBuf.toBuffer()).toString('base64')
        console.log('NodeBase64:', nodeBase64)

        nc.publish(reply, nodeBase64)
        break
      }
      case 'manifest_cid_by_tea_id': {
        const teaId = toHex(Buffer.from(msg, 'base64'), { addPrefix: true })
        console.log('manifest_cid_by_tea_id, teaId:', teaId)
        const nodeObj = await api.query.tea.manifest(teaId)

        if (nodeObj.isNone) {
          console.log('No such manifest found')
          nc.publish(reply, '')
        }
        let cid = nodeObj.toJSON()
        cid = Buffer.from(cid.slice(2), 'hex')

        nc.publish(reply, cid)

        break
      }
      case 'node_profile_by_tea_id': {
        const teaId = toHex(Buffer.from(msg, 'base64'), { addPrefix: true })
        const nodeObj = await api.query.tea.nodes(teaId)
        if (nodeObj.isNone) {
          console.log('No such node found')
          console.log('teaId:', teaId)
          nc.publish(reply, 'no_such_node_found')
          break
        }
        const node = nodeObj.toJSON()
        const urls = []
        if (node.urls) {
          node.urls.forEach((url, i) => {
            urls.push(Buffer.from(url.slice(2), 'hex').toString())
          })
        }
        const raNodes = []
        if (node.raNodes) {
          node.raNodes.forEach((raNode, i) => {
            raNodes.push({
              teaId: Buffer.from(raNode[0], 'hex'),
              isPass: Boolean(raNode[1])
            })
          })
        }
        const nodeProfile = {
          ephemeralPublicKey: Buffer.from(node.ephemeralId.slice(2), 'hex'),
          profileCid: Buffer.from(node.profileCid.slice(2), 'hex').toString(),
          teaId: Buffer.from(node.teaId.slice(2), 'hex'),
          publicUrls: urls,
          peerId: Buffer.from(node.peerId.slice(2), 'hex').toString(),
          raNodes,
          status: node.status.toString()
        }
        console.log('Lookup node profile:', JSON.stringify(nodeProfile))
        const nodeBuf = new proto.RAProtobuf('NodeProfile')
        nodeBuf.payload(nodeProfile)
        const nodeBase64 = Buffer.from(nodeBuf.toBuffer()).toString('base64')
        console.log('NodeBase64:', nodeBase64)

        nc.publish(reply, nodeBase64)
        break
      }
      case 'update_generate_key_result': {
        const newRequestBuf = new proto.DelegateProtobuf('UpdateKeyGenerationResult')
        const updateKeyGenerationResultRequest = newRequestBuf.decode(Buffer.from(msg, 'base64'))

        console.log("update_generate_key_result msg:", updateKeyGenerationResultRequest)

        const taskId = '0x' + updateKeyGenerationResultRequest.taskId
        const delegatorNonce = toHex(updateKeyGenerationResultRequest.delegatorNonce, { addPrefix: true })
        const multiSigAccount = toHex(updateKeyGenerationResultRequest.multiSigAccount, { addPrefix: true })
        const p2PublicKey = toHex(updateKeyGenerationResultRequest.publicKey, { addPrefix: true })
        const deploymentIds = []
        if (updateKeyGenerationResultRequest.deploymentIds) {
          updateKeyGenerationResultRequest.deploymentIds.forEach((id, i) => {
            deploymentIds.push('0x' + id)
          })
        }

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.gluon.updateGenerateAccountWithoutP3Result(taskId,
            delegatorNonce, p2PublicKey, deploymentIds, multiSigAccount)
            .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
              if (status.isInBlock) {
                console.log('update generate key result ' + teaId)
                nc.publish(reply, JSON.stringify({ status, teaId }))
              } else {
                console.log('Status of transfer: ' + status.type)
              }

              events.forEach(({ phase, event: { data, method, section } }) => {
                console.log(phase.toString() + ' : ' + section + '.' + method + ' ' + data.toString())
              })
            })
        console.log('send update_generate_account_without_p3_result tx')
        break
      }
      case 'update_sign_transaction_result': {
        const newRequestBuf = new proto.DelegateProtobuf('UpdateSignTransactionResult')
        const updateSignTransactionRequest = newRequestBuf.decode(Buffer.from(msg, 'base64'))

        const taskId = toHex(updateSignTransactionRequest.taskId, { addPrefix: true })
        const delegatorNonce =  toHex(updateSignTransactionRequest.delegatorNonce, { addPrefix: true })
        const succeed = updateSignTransactionRequest.succeed

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.gluon.updateSignTransactionResult(taskId, delegatorNonce, succeed)
            .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
              if (status.isInBlock) {
                console.log('update sign transaction result ' + teaId)
                nc.publish(reply, JSON.stringify({ status, teaId }))
              } else {
                console.log('Status of transfer: ' + status.type)
              }

              events.forEach(({ phase, event: { data, method, section } }) => {
                console.log(phase.toString() + ' : ' + section + '.' + method + ' ' + data.toString())
              })
            })
      }
      case 'deposit_info': {
        const newRequestBuf = new proto.DelegateProtobuf('DepositInfoRequest')
        const newRequest = newRequestBuf.decode(Buffer.from(msg, 'base64'))

        const accountId = newRequest.accountId
        const delegatorTeaId = toHex(newRequest.delegatorTeaId, { addPrefix: true })

        const depositInfoObj = await api.query.tea.depositMap([accountId, delegatorTeaId])
        if (depositInfoObj.isNone) {
          console.log('No such deposit found')
          nc.publish(reply, '')
          break
        }

        const depositInfo = depositInfoObj.toJSON()
        // console.log("Deposit info:", depositInfo);
        const amount = new BN(depositInfo.amount.slice(2), 'hex')
        // console.log("amount", amount);
        const depositInfoResponse = {
          accountId,
          delegatorTeaId: Buffer.from(depositInfo.delegatorTeaId.slice(2), 'hex'),
          delegatorEphemeralId: Buffer.from(depositInfo.delegatorEphemeralId.slice(2), 'hex'),
          delegatorSignature: Buffer.from(depositInfo.delegatorSignature.slice(2), 'hex'),
          amount: amount.div(unit).toNumber(),
          expiredTime: parseInt(depositInfo.expireTime, 10)
        }

        console.log('depositInfoResponse:', JSON.stringify(depositInfoResponse))
        const responseBuf = new proto.DelegateProtobuf('DepositInfoResponse')
        responseBuf.payload(depositInfoResponse)
        const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
        console.log('deposit_info DepositInfoResponse Base64', responseBase64)

        nc.publish(reply, responseBase64)
        break
      }
      case 'add_new_data': {
        var protoMsg = Buffer.from(msg, 'base64')
        const newDataBuf = new proto.DelegateProtobuf('AddNewDataRequest')
        const newData = newDataBuf.decode(protoMsg)
        console.log(newData)

        var delegatorEphemeralId = toHex(newData.data.delegatorEphemeralId, { addPrefix: true })
        var deploymentId = toHex(Buffer.from(newData.data.deploymentId), { addPrefix: true })
        const dataCid = toHex(Buffer.from(newData.data.dataCid), { addPrefix: true })
        const descriptionCid = toHex(Buffer.from(newData.data.descriptionCid), { addPrefix: true })
        const capCid = toHex(Buffer.from(newData.data.capCid), { addPrefix: true })

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.tea.addNewData(delegatorEphemeralId, deploymentId, dataCid, descriptionCid, capCid)
          .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
            if (status.isInBlock) {
              console.log('Included at block hash', status.asInBlock.toHex())
              console.log('Events:')
              events.forEach(({ event: { data, method, section }, phase }) => {
                console.log('\t', phase.toString(), `: ${section}.${method}`, data.toString())
              })
            } else if (status.isFinalized) {
              console.log('Finalized block hash', status.asFinalized.toHex())
            }
          })
        console.log('send add_new_data tx')
        break
      }
      case 'settle_accounts': {
        const protoMsg = Buffer.from(msg, 'base64')
        const newRequestBuf = new proto.DelegateProtobuf('SettleAccountsRequest')
        const newRequest = newRequestBuf.decode(protoMsg)
        // console.log(newRequest);

        const employer = newRequest.employer
        const delegatorTeaId = toHex(newRequest.delegatorTeaId, { addPrefix: true })
        const delegatorEphemeralId = toHex(newRequest.delegatorEphemeralId, { addPrefix: true })
        const errandUuid = toHex(Buffer.from(newRequest.errandUuid), { addPrefix: true })
        const errandJsonCid = toHex(Buffer.from(newRequest.errandJsonCid), { addPrefix: true })
        const employerSignature = toHex(newRequest.employerSignature, { addPrefix: true })
        const executorEphemeralId = toHex(newRequest.executorEphemeralId, { addPrefix: true })
        const expiredTime = parseInt(newRequest.expiredTime, 10)
        const delegateSignature = toHex(newRequest.delegateSignature, { addPrefix: true })
        const resultCid = toHex(Buffer.from(newRequest.resultCid), { addPrefix: true })
        const executorSingature = toHex(newRequest.executorSingature, { addPrefix: true })

        const bills = []
        newRequest.bills.forEach((bill, i) => {
          bills.push([bill.accountId, new BN(bill.payment, 10).mul(unit).toString()])
        })

        // const bills = [['5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', 10], ['5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', 10]];

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.tea.settleAccounts(
          employer,
          delegatorTeaId,
          delegatorEphemeralId,
          errandUuid,
          errandJsonCid,
          employerSignature,
          executorEphemeralId,
          expiredTime,
          delegateSignature,
          resultCid,
          executorSingature,
          bills)
          .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
            if (status.isInBlock) {
              console.log('Included at block hash', status.asInBlock.toHex())
              console.log('Events:')
              events.forEach(({ event: { data, method, section }, phase }) => {
                console.log('\t', phase.toString(), `: ${section}.${method}`, data.toString())
              })
            } else if (status.isFinalized) {
              console.log('Finalized block hash', status.asFinalized.toHex())
            }
          })
        console.log('send settle_accounts tx')
        break
      }
      case 'update_runtime_activity': {
        const protoMsg = Buffer.from(msg, 'base64')
        const newRequestBuf = new proto.DelegateProtobuf('UpdateRuntimeActivity')
        const newRequest = newRequestBuf.decode(protoMsg)
        console.log(newRequest)

        const teaId = toHex(newRequest.teaId, { addPrefix: true })
        const cid = toHex(Buffer.from(newRequest.cid), { addPrefix: true })
        const ephemeralId = toHex(newRequest.ephemeralId, { addPrefix: true })
        const signature = toHex(newRequest.signature, { addPrefix: true })
        var delegatePubkey = toHex(newRequest.delegatePubKey, { addPrefix: true })

        const nonce = await api.rpc.system.accountNextIndex(ac.address)
        await api.tx.tea.updateRuntimeActivity(teaId, cid, ephemeralId, signature, delegatePubkey)
          .signAndSend(ac, {nonce: nonce},({ events = [], status }) => {
            if (status.isInBlock) {
              console.log('Included at block hash', status.asInBlock.toHex())
              console.log('Events:')
              events.forEach(({ event: { data, method, section }, phase }) => {
                console.log('\t', phase.toString(), `: ${section}.${method}`, data.toString())
              })
            } else if (status.isFinalized) {
              console.log('Finalized block hash', status.asFinalized.toHex())
            }
          })
        console.log('send add_new_data tx')
        break
      }
      case 'runtime_activity_by_tea_id': {
        const teaId = '0x' + Buffer.from(msg, 'base64').toString('hex')
        console.log('runtime_activity_by_tea_id, teaId:', teaId)
        const runtimeActivityObj = await api.query.tea.runtimeActivities(teaId)

        if (runtimeActivityObj.isNone) {
          console.log('No such runtime activity found')
          nc.publish(reply, '')
        }
        const runtimeActivity = runtimeActivityObj.toJSON()

        const runtimeActivityResponse = {
          teaId: Buffer.from(runtimeActivity.teaId.slice(2), 'hex'),
          cid: Buffer.from(runtimeActivity.cid.slice(2), 'hex').toString(),
          updateHeight: parseInt(runtimeActivity.updateHeight, 10)
        }

        console.log('Look up runtime activity:', JSON.stringify(runtimeActivityResponse))
        const responseBuf = new proto.DelegateProtobuf('RuntimeActivityResponse')
        responseBuf.payload(runtimeActivityResponse)
        const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
        console.log('RuntimeActivityResponse Base64', responseBase64)

        nc.publish(reply, responseBase64)
        break
      }
      case 'update_manifest_cid': {
        // TODO
        break
      }
      case 'get_delegates': {
        const uProtoMsg = Buffer.from(msg, 'base64')
        const getDelegatesRequestBuf = new proto.DelegateProtobuf('GetDelegatesRequest')
        const getDelegatesRequest = getDelegatesRequestBuf.decode(uProtoMsg)
        console.log("get_delegates start:", getDelegatesRequest.start, "limit:", getDelegatesRequest.limit);

        const delegates = await api.rpc.tea.getDelegates(getDelegatesRequest.start, getDelegatesRequest.limit)
        console.log('tea_getDelegates result:', delegates.toString())
        const delegateItems = []
        for (let i = 0; i < delegates.length; i++) {
          const delegateItem = {
            teaId: Buffer.from(delegates[i][1], 'hex'),
            peerId: Buffer.from(delegates[i][2].toString().slice(2), 'hex').toString()
          }
          console.log('teaId:', delegates[i][1].toString())
          delegateItems.push(delegateItem)
        }

        const getDelegatesResponse = {
          delegates: delegateItems,
        }

        console.log('newGetDelegatesResponse:', JSON.stringify(getDelegatesResponse))
        const responseBuf = new proto.DelegateProtobuf('GetDelegatesResponse')
        responseBuf.payload(getDelegatesResponse)
        const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
        console.log('GetDelegatesResponse Base64', responseBase64)
        nc.publish(reply, responseBase64)
        break
      }
      case 'get_deployment_ids': {
        const uProtoMsg = Buffer.from(msg, 'base64')
        const getDeploymentIdsRequestBuf = new proto.DelegateProtobuf('GetDeploymentIds')
        const getDeploymentIdsRequest = getDeploymentIdsRequestBuf.decode(uProtoMsg)
        console.log("get_deployment_ids multiSigAccount:", getDeploymentIdsRequest.multiSigAccount);

        const assetInfo = await api.query.gluon.assets(getDeploymentIdsRequest.multiSigAccount)
        console.log('get_deployment_ids result:', assetInfo.toString())

        const asset = assetInfo.toJSON()
        const AssetInfo = {
          sender: Buffer.from(asset.owner, 'hex'),
          p2: Buffer.from(asset.p2.slice(2), 'hex'),
          p2DeploymentIds: asset.deploymentIds,
        }

        const getDeploymentIdsResponse = {
          assetInfo: AssetInfo
        }

        console.log('newGetDeploymentIdsResponse:', JSON.stringify(getDeploymentIdsResponse))
        const responseBuf = new proto.DelegateProtobuf('GetDeploymentIdsResponse')
        responseBuf.payload(getDeploymentIdsResponse)
        const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
        console.log('GetDeploymentIdsResponse Base64', responseBase64)
        nc.publish(reply, responseBase64)
        break
      }
      default:
        nc.publish(reply, JSON.stringify(['action_does_not_support']))
    }
  })
}

function handle_new_header (header) {
  nc.publish(`layer1.chain.newheader.${header.number}`, `${header.hash}`)
}

function handle_events (events) {
  // Loop through the Vec<EventRecord>
  events.forEach((record) => {
    // Extract the phase, event and the event types
    const { event, phase } = record
    const types = event.typeDef

    if (event.section === 'tea' | event.section === 'gluon') {
      console.log('Received ', event.section, 'events:')

      const eventData = {}
      // Loop through each of the parameters, displaying the type and data
      event.data.forEach((data, index) => {
        // console.log(`\t\t\t${types[index].type}: ${data.toString()}`);
        eventData[types[index].type] = data
      })
      console.log('eventData:', JSON.stringify(eventData))

      switch (event.method) {
        case 'UpdateNodeProfile': {
          const urls = []
          if (eventData.Node.urls) {
            eventData.Node.urls.forEach((url, i) => {
              urls.push(Buffer.from(url, 'hex').toString())
            })
          }
          const raNodes = []
          if (eventData.Node.raNodes) {
            eventData.Node.raNodes.forEach((raNode, i) => {
              raNodes.push({
                teaId: Buffer.from(raNode[0], 'hex'),
                isPass: Boolean(raNode[1])
              })
            })
          }
          const nodeProfile = {
            teaId: Buffer.from(eventData.Node.teaId, 'hex'),
            ephemeralPublicKey: Buffer.from(eventData.Node.ephemeralId, 'hex'),
            profileCid: Buffer.from(eventData.Node.profileCid, 'hex').toString(),
            publicUrls: urls,
            peerId: Buffer.from(eventData.Node.peerId, 'hex').toString(),
            raNodes,
            status: eventData.Node.status.toString()
          }

          const nodeResponse = {
            accountId: eventData.AccountId.toString(),
            nodeProfile
          }
          const buf = new proto.RAProtobuf('TeaNodeResponse')
          buf.payload(nodeResponse)
          const responseBase64 = Buffer.from(buf.toBuffer()).toString('base64')
          console.log('TeaNodeResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'NewNodeJoined': {
          const addNewNodeResponse = {
            accountId: eventData.AccountId.toString(),
            teaId: Buffer.from(eventData.Node.teaId, 'hex')
          }

          const responseBuf = new proto.DelegateProtobuf('AddNewNodeResponse')
          responseBuf.payload(addNewNodeResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('AddNewNodeResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'NewDataAdded': {
          const data = {
            delegatorEphemeralId: Buffer.from(eventData.Data.delegatorEphemeralId, 'hex'),
            deploymentId: Buffer.from(eventData.Data.deploymentId, 'hex').toString(),
            dataCid: Buffer.from(eventData.Data.cid, 'hex').toString(),
            descriptionCid: Buffer.from(eventData.Data.description, 'hex').toString(),
            capCid: Buffer.from(eventData.Data.capChecker, 'hex').toString()
          }

          const newDataResponse = {
            accountId: Buffer.from(eventData.AccountId, 'hex'),
            data
          }

          console.log('newDataResponse:', JSON.stringify(newDataResponse))
          const newDataResponseBuf = new proto.DelegateProtobuf('AddNewDataResponse')
          newDataResponseBuf.payload(newDataResponse)
          const newDataResponseBase64 = Buffer.from(newDataResponseBuf.toBuffer()).toString('base64')
          console.log('Event NewDataAdded Base64:', newDataResponseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, newDataResponseBase64)
          break
        }
        case 'NewDepositAdded': {
          const newDepositResponse = {
            accountId: eventData.AccountId.toString(),
            delegatorTeaId: Buffer.from(eventData.Deposit.delegatorTeaId, 'hex'),
            delegatorEphemeralId: Buffer.from(eventData.Deposit.delegatorEphemeralId, 'hex'),
            delegatorSignature: Buffer.from(eventData.Deposit.delegatorSignature, 'hex'),
            amount: eventData.Deposit.amount.div(unit).toNumber(),
            expiredTime: parseInt(eventData.Deposit.expireTime, 10)
          }

          console.log('newDepositResponse:', JSON.stringify(newDepositResponse))
          const responseBuf = new proto.DelegateProtobuf('DepositInfoResponse')
          responseBuf.payload(newDepositResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('Event NewDepositAdded Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'SettleAccounts': {
          const bills = []
          eventData.Bill.bills.forEach((bill, i) => {
            bills.push({
              accountId: bill[0].toString(),
              payment: bill[1].div(unit).toNumber()
            })
          })
          const settleAccountsResponse = {
            accountId: eventData.AccountId.toString(),
            employer: eventData.Bill.employer.toString(),
            delegatorTeaId: Buffer.from(eventData.Bill.delegatorTeaId, 'hex'),
            delegatorEphemeralId: Buffer.from(eventData.Bill.delegatorEphemeralId, 'hex'),
            errandUuid: Buffer.from(eventData.Bill.errandUuid, 'hex').toString(),
            errandJsonCid: Buffer.from(eventData.Bill.errandJsonCid, 'hex').toString(),
            bills,
            executorEphemeralId: Buffer.from(eventData.Bill.executorEphemeralId, 'hex'),
            expiredTime: parseInt(eventData.Bill.expiredTime),
            resultCid: Buffer.from(eventData.Bill.resultCid, 'hex').toString()
          }
          console.log('settleAccountsResponse:', JSON.stringify(settleAccountsResponse))

          const responseBuf = new proto.DelegateProtobuf('SettleAccountsResponse')
          responseBuf.payload(settleAccountsResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('Event SettleAccounts Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'CommitRaResult': {
          const commitRaResultResponse = {
            accountId: eventData.AccountId.toString(),
            teaId: Buffer.from(eventData.RaResult.teaId, 'hex'),
            targetTeaId: Buffer.from(eventData.RaResult.targetTeaId, 'hex'),
            isPass: Boolean(eventData.RaResult.isPass),
            targetStatus: eventData.RaResult.targetStatus.toString()
          }

          console.log('newCommitRaResultResponse:', JSON.stringify(commitRaResultResponse))
          const responseBuf = new proto.RAProtobuf('CommitRaResultResponse')
          responseBuf.payload(commitRaResultResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('CommitRaResultResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'UpdateRuntimeActivity': {
          if (eventData.RuntimeActivity.cid.isSome) {
            cid = Buffer.from(eventData.RuntimeActivity.cid.unwrap(), 'hex').toString()
          }
          const runtimeActivityResponse = {
            // accountId: eventData.AccountId.toString(),
            teaId: Buffer.from(eventData.RuntimeActivity.teaId, 'hex'),
            cid: Buffer.from(cid, 'hex').toString(),
            updateHeight: parseInt(eventData.RuntimeActivity.updateHeight, 10)
          }

          console.log('newRuntimeActivityResponse:', JSON.stringify(runtimeActivityResponse))
          const responseBuf = new proto.DelegateProtobuf('RuntimeActivityResponse')
          responseBuf.payload(runtimeActivityResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('RuntimeActivityResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'AccountGenerationRequested': {
          console.log('newKeyGenerationResponse start')
          const keyGenerationData = {
            n: parseInt(eventData.AccountGenerationDataWithoutP3.n, 10),
            k: parseInt(eventData.AccountGenerationDataWithoutP3.k, 10),
            keyType: Buffer.from(eventData.AccountGenerationDataWithoutP3.keyType, 'hex').toString(),
            delegatorTeaNonceHash:  Buffer.from(eventData.AccountGenerationDataWithoutP3.delegatorNonceHash, 'hex'),
            delegatorTeaNonceRsaEncryption:  Buffer.from(eventData.AccountGenerationDataWithoutP3.delegatorNonceRsa, 'hex'),
          }

          const payment = {}
          const keyGenerationResponse = {
            taskId: Buffer.from(eventData.Cid, 'hex').toString(),
            dataAdhoc: keyGenerationData,
            payment: payment,
            p1PublicKey: Buffer.from(eventData.AccountGenerationDataWithoutP3.p1, 'hex'),
          }

          console.log('newKeyGenerationResponse:', JSON.stringify(keyGenerationResponse))
          const responseBuf = new proto.DelegateProtobuf('KeyGenerationResponse')
          responseBuf.payload(keyGenerationResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('KeyGenerationResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case 'SignTransactionRequested': {
          const payment = {}
          const signTransactionData = {
            transactionData: Buffer.from(eventData.SignTransactionTask.taskData.dataAdhoc, 'hex'),
            delegatorTeaNonceHash: Buffer.from(eventData.SignTransactionTask.taskData.delegatorNonceHash, 'hex'),
            delegatorTeaNonceRsaEncryption: Buffer.from(eventData.SignTransactionTask.taskData.delegatorNonceRsa, 'hex'),
          }
          const signTransactionResponse = {
            taskId: Buffer.from(eventData.SignTransactionTask.taskId, 'hex').toString(),
            dataAdhoc: signTransactionData,
            payment: payment,
            p1Signature: Buffer.from(eventData.SignTransactionTask.p1Signature, 'hex'),
            multiSigAccount: Buffer.from(eventData.SignTransactionTask.multisigAddress, 'hex'),
          }

          console.log('newSignTransactionResponse:', JSON.stringify(signTransactionResponse))
          const responseBuf = new proto.DelegateProtobuf('SignTransactionResponse')
          responseBuf.payload(signTransactionResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('SignTransactionResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        case "AssetGenerated": {
          const p2DeploymentIds = []
          if (eventData.Asset.deploymentIds) {
            eventData.Asset.deploymentIds.forEach((id, i) => {
              p2DeploymentIds.push(Buffer.from(id, 'hex').toString())
            })
          }
          const AssetInfo = {
            sender: Buffer.from(eventData.Asset.owner, 'hex'),
            p2: Buffer.from(eventData.Asset.p2, 'hex'),
            p2DeploymentIds: p2DeploymentIds,
          }

          const assetGeneratedResponse = {
            taskId:Buffer.from(eventData.Cid, 'hex').toString(),
            multiSigAccount: Buffer.from(eventData.MultiSigAccount, 'hex'),
            assetInfo: AssetInfo,
          }

          console.log('newAssetGeneratedResponse:', JSON.stringify(assetGeneratedResponse))
          const responseBuf = new proto.DelegateProtobuf('AssetGeneratedResponse')
          responseBuf.payload(assetGeneratedResponse)
          const responseBase64 = Buffer.from(responseBuf.toBuffer()).toString('base64')
          console.log('AssetGeneratedResponse Base64', responseBase64)

          nc.publish(`layer1.event.${event.section}.${event.method}`, responseBase64)
          break
        }
        default:
      }
    }
  })
}

main().catch((error) => {
  console.error(error)
  process.exit(-1)
})
