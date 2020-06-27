const types = {
      Weight: "u32",
      Address: "AccountId",
      TeaId: "Bytes",
      PeerId: "Bytes",
      TaskId: "H256",
      Result: "Bytes",
      Node: {
            "TeaId": "TeaId",
            "Peers": "Vec<PeerId>"
      },
      Model: {
            "account": "AccountId",
            "payment": "u32",
            "cid": "H256"
      },
      Task: {
            "delegate_node": "TeaId",
            "model_cid": "Bytes",
            "body_cid": "Bytes",
            "payment": "Balance"
      }
}

module.exports = types;