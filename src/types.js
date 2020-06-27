const types = {
      Weight: "u32",
      Address: "AccountId",
      TeaId: "Bytes",
      PeerId: "Bytes",
      RefNum: "Bytes",
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
            "ref_num": "Bytes",
            "delegate_node": "TeaId",
            "model_cid": "Bytes",
            "body_cid": "Bytes",
            "payment": "Balance"
      }
}

module.exports = types;