module net.pincette.kafka.json {
  requires net.pincette.json;
  requires kafka.clients;
  requires com.fasterxml.jackson.dataformat.cbor;
  requires java.json;
  requires net.pincette.common;

  exports net.pincette.kafka.json;
}
