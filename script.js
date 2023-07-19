window.onload = function(){
    fetchData();
}

async function fetchData() {
    var lineage_data = []
    try {
        lineage_data = await Func();
    } catch (error) {
        console.error(error);
    }
    var tables = Object.keys(lineage_data); // Replace with your own array of strings
    var container = document.getElementById("container");
    container.innerHTML = "";

    var offsetX = 200; // Adjust the vertical separation between boxes
    leftOffset = 100
    topOffset = 300
    colCoordinates = {}
    normalModelCnt = 0
    sourceTableCnt = 0

    tables.forEach(function(table, index) {
      var src_table = false
      var box = document.createElement("div");
      box.className = "box";
      box.innerHTML = "<b>" + table + "</b>";
      var columns = Object.keys(lineage_data[table])
      if(columns[0] == '0'){
        columns = lineage_data[table]
        src_table = true
        sourceTableCnt += 1
      }
      else {
        normalModelCnt += 1
      }
      console.log(sourceTableCnt, normalModelCnt)
      if (index == 0) {
        normalModelCnt -= 1
        box.style.left = leftOffset + "px"; // Set initial vertical position
        box.style.top = topOffset + "px"; // Set initial vertical position
      }
      else if (src_table) {
        box.style.left = "800px"; // Set initial vertical position
        box.style.top = (offsetX * sourceTableCnt) + "px"; // Set initial vertical position
      }
      else {
        box.style.left = "400px"; // Set initial vertical position
        box.style.top = (offsetX * normalModelCnt) + "px"; // Set initial vertical position
      }
      columns.forEach(function(column, idx) {
        tempTop = (box.style.topOffset + 45) + "px";
        tempLeft = (box.style.left + 180) + "px";
        box.innerHTML += "<hr />" + column //+ "(" + tempTop + " ," + tempLeft + ")"
      })
      box.innerHTML += "<hr />"
      container.appendChild(box);
      makeDraggable(box);
    });

    function makeDraggable(element) {
      var pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;
      element.onmousedown = dragMouseDown;

      function dragMouseDown(e) {
        e = e || window.event;
        e.preventDefault();
        pos3 = e.clientX;
        pos4 = e.clientY;
        document.onmouseup = closeDragElement;
        document.onmousemove = elementDrag;
      }

      function elementDrag(e) {
        e = e || window.event;
        e.preventDefault();
        pos1 = pos3 - e.clientX;
        pos2 = pos4 - e.clientY;
        pos3 = e.clientX;
        pos4 = e.clientY;
        element.style.top = (element.offsetTop - pos2) + "px";
        element.style.left = (element.offsetLeft - pos1) + "px";
      }

      function closeDragElement() {
        document.onmouseup = null;
        document.onmousemove = null;

        // Update the dropped position
        var dropzone = document.getElementById("container");
        var rect = dropzone.getBoundingClientRect();
        var newLeft = element.offsetLeft - rect.left;
        var newTop = element.offsetTop - rect.top;
        element.style.left = newLeft + "px";
        element.style.top = newTop + "px";
        console.log(element.style.top)
      }
    }
  };

function Func() {
    return fetch("./lineage_op.json")
        .then((res) => {
        return res.json();
    })
    .then((data) => {
        return data
    });
}