const { ApiPromise, Keyring } = require('@polkadot/api')
const { stringToU8a, u8aToHex } = require('@polkadot/util')
const types = require('../src/types')
const rpc = require('../src/rpc')
const proto = require('../src/proto')

async function main () {
  const api = await ApiPromise.create({
    types,
    rpc
  })

  const multiSigAccount = u8aToHex(stringToU8a('testAddress'));
  const assetInfo = await api.query.gluon.assets(multiSigAccount);
  console.log('get_deployment_ids result:', assetInfo.toString());

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

  console.log('get deployment ids finished')
}

main()
