const types = {
      Weight: "u32",
      Address: "AccountId",
      TeaId: "[u8; 32]",
      PeerId: "Bytes",
      RefNum: "H256",
      Result: "Bytes",
      Node: {
            "teaId": "TeaId",
            "peers": "Vec<PeerId>"
      },
      Model: {
            "account": "AccountId",
            "payment": "u32",
            "cid": "Bytes"
      },
      Task: {
            "refNum": "RefNum",
            "delegateTeaId": "TeaId",
            "modelCid": "Bytes",
            "bodyCid": "Bytes",
            "payment": "Balance"
      }
}

module.exports = types;