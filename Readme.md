# bitquery-protobuf
bitquery-protobuf is an npm package for consuming Kafka Protobuf messages from Bitquery. Bitquery is an onchain data provider firm that provides blockchain data solutions in the form of GraphQL APIs, Websocket and Kafka streams. The Kafka Protobuf streams is useful when having low latency is of the utmost importance. The purpose of this package is to streamline the development process without worrying about setting up Kafka stream or downloading and decoding `.proto` files.

## Installation
```shell
npm install bitquery-protobuf
```
# Usage

```js
const {runProto} = require('bitquery-protobuf');

runProto("<username>", "<password>", "topic");
```

- \<username> & \<password>: The credentials to access the Bitquery Kafka stream. To get your credentials contact - sales@bitquery.io

- \<topic>: The topic of the Kafka Protobuf stream that the user wants to subscribe.

This package is a `Getting Started` package for the protobuf streams that prints the message recieved from stream. Contact our team if you need to build a customised solution on top of that.

To read more about Bitquery Kafka solutions checkout their official [documentation](https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/).

## Contributing

Contributions are welcome! Feel free to submit issues and pull requests [here](https://github.com/Kshitij0O7/bitquery-protobuf).

License

ISC License © 2025 bitquery-protobuf