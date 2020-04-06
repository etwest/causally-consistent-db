var payload = "{}";

var nodes   = document.currentScript.getAttribute("nodes");
var active_nodes = new Set();
for(var i = 0; i < nodes; i++) {
  active_nodes.add(i);
}

$("#node").on("change", function() {
  var node = $("#node").val();
  if (active_nodes.has(parseInt(node))) {
    $("#node_error").attr("hidden", true);
  } else {
    $("#node_error").removeAttr("hidden");
  }
})

function clearDiv() {
  // large divs which wrap the options for each type
  $("#querying-key").attr("hidden", true);
  $("#querying-shard").attr("hidden", true);
  $("#querying-node").attr("hidden", true);

  // inputs related to specific operations
  $("#value-wrap").attr("hidden", true);
  $("#id-wrap").attr("hidden", true);
  $("#num-wrap").attr("hidden", true);
  $("#ip-wrap").attr("hidden", true);
}

$("#q-type").on("change", function() {
  switch($("#q-type").val()) {
    case "KVS":
      clearDiv();
      $("#querying-key").removeAttr("hidden");
      break;
    case "SHARDS":
      clearDiv();
      $("#querying-shard").removeAttr("hidden");
      break;
    case "NODES":
      clearDiv();
      $("#querying-node").removeAttr("hidden");
      break;
  }
});

$("#OP-key").on("change", function() {
  switch($("#OP-key").val()) {
    case "PUT":
      $("#value-wrap").removeAttr("hidden");
      break;
    default:
      $("#value-wrap").attr("hidden", true);
      break;
  }
});

$("#OP-shard").on("change", function() {
  switch($("#OP-shard").val()) {
    case "MEMBERS":
    case "COUNT":
      $("#id-wrap").removeAttr("hidden");
      $("#num-wrap").attr("hidden", true);
      break;
    case "NUMBER":
      $("#num-wrap").removeAttr("hidden");
      $("#id-wrap").attr("hidden", true);
      break;
    default:
      $("#id-wrap").attr("hidden", true);
      $("#num-wrap").attr("hidden", true);
      break;
  }
});

$("#OP-node").on("change", function() {
  switch($("#OP-node").val()) {
    case "ADD NODE":
      $("#ip-wrap").removeAttr("hidden");
      
      break;
    case "DELETE NODE":
      $("#ip-wrap").removeAttr("hidden");
      break;
    default:
      $("#ip-wrap").attr("hidden", true);
      break;
  }
});

// This sends the form information to the kvs endpoints
$( "#query-form" ).submit(async function( event ) {
  event.preventDefault();
  var URI = "";
  var DATA = {payload:payload};
  var TYPE = "GET";

  var query = "";

  switch($("#q-type").val()) {
    case "KVS":
      key   = $("#key").val();
      URI   = "/keyValue-store/" + key;
      TYPE  = $("#OP-key").val();
      query = "KEY - " + TYPE + " - " + key;
      if (TYPE == "PUT") {
        DATA["val"] = $("#value").val();
        query += " - " + $("#value").val();
      }
      if (TYPE == "SEARCH") {
        TYPE = "GET";
        URI = "/keyValue-store/search/" + key;
      }
      break;
    case "SHARDS":
      query = "SHARD - " + $("#OP-shard").val();
      switch($("#OP-shard").val()) {
        case "MY_ID":
          TYPE = "GET";
          URI = "/shard/my_id";
          break;
        case "ALL_ID":
          TYPE = "GET";
          URI = "/shard/all_ids";
          break;
        case "MEMBERS":
          TYPE = "GET";
          id  = $("#shard-id").val();
          query += " " + id;
          URI = "/shard/members/" + id;
          break;
        case "COUNT":
          TYPE = "GET";
          id  = $("#shard-id").val();
          query += " " + id;
          URI = "/shard/count/" + id;
          break;
        case "NUMBER":
          TYPE = "PUT";
          num  = $("#shard-num").val()
          query += " " + num;
          URI = "/shard/changeShardNumber";
          DATA["num"] = num;
          break;
      }
      break;
    case "NODES":
        query = "NODES - " + $("#OP-node").val();
        URI = "/view";
        switch($("#OP-node").val()) {
          case "GET VIEW":
            TYPE = "GET";
            break;
          case "ADD NODE":
            TYPE = "PUT";
            var ip_port = $("#ip-port").val();
            DATA["ip_port"] = ip_port;
            query += " " + ip_port;

            // calculate the node number
            var ip = ip_port.split(":")[0];
            var node_num = parseInt(ip.split(".")[3]) - 10;
            // insert the new node to the list if not in the list
            if (active_nodes.has(node_num)) {
              console.log("This node is in the list already!");
            }
            else {
              active_nodes.add(node_num);
            }
            break;
          case "DELETE NODE":
            TYPE = "DELETE";
            var ip_port = $("#ip-port").val();
            DATA["ip_port"] = ip_port;
            query += " " + ip_port;
            // calculate the node number
            var ip = ip_port.split(":")[0];
            var node_num = parseInt(ip.split(".")[3]) - 10;
            // delete the node from the list
            active_nodes.delete(node_num);
            break;
        }
      break;
  }
  // send to node 8080 + node number
  PORT = 8080 + parseInt($("#node").val());
  URL = "http://localhost:" + PORT.toString(10) + URI;

  $.ajax({
    url:  URL,
    type: TYPE,
    data: DATA,
    dataType: "json",
    success: function(result){
      var results = document.getElementById("results");
      var text;
      // replace with just msg if that is present
      if(result.hasOwnProperty("msg")) {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " result: " + result["msg"]);
      }
      else if(result.hasOwnProperty("value")) {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " value returned: " + result["value"]);
      }
      else if(result.hasOwnProperty("isExists")) {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " value returned: " + result["isExists"]);
      }
      else {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " result: " + JSON.stringify(result));
      }
      if($("#q-type").val() == "SHARDS" && $("#OP-shard").val() == "NUMBER") {
        $("#shard-id").attr("max", $("#shard-num").val() - 1)
      }

      results.appendChild(text);
      results.innerHTML += "<br>";
      console.log(result);
      // scroll to the bottom the the results div
      $("#results").scrollTop($("#results")[0].scrollHeight);
      // set the payload to what was returned by the server
      if (result.hasOwnProperty("payload")) {
        payload = result["payload"];
      }
    },
    error: function(error) {
      var results = document.getElementById("results");
      var text;
      if (error["responseText"] == undefined) {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " error: Could not contact node");
      }
      else if (error["responseJSON"].hasOwnProperty("error")) {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " error: " + error["responseJSON"]["error"]);
      }
      else if (error["responseJSON"].hasOwnProperty("msg")) {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " error: " + error["responseJSON"]["msg"]);
      }
      else {
        text      = document.createTextNode(query + " to node " + $("#node").val() + " error: " + error["responseText"]);
      }
      
      results.appendChild(text);
      results.innerHTML += "<br>";
      // scroll to the bottom the the results div
      $("#results").scrollTop($("#results")[0].scrollHeight);
      console.log('Error', error);
    }
  })
});
