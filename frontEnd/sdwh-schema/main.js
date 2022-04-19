numDimensions = 0;
numFactTableColumns = 0;

const addDimensionTable = () => {
    numDimensions += 1;

    divId = 'd' + numDimensions;
    dimensionNameId = 'dname' + numDimensions;
    dataLocationNameId = 'dlname' + numDimensions;

    parent = document.getElementById("dimensions");

    dimensionDiv = document.createElement("div");
    dimensionDiv.setAttribute('id', divId);
    dimensionDiv.setAttribute('class', 'dimension');

    dimensionLabel = document.createElement("label");
    dimensionLabel.setAttribute('for', dimensionNameId);
    dimensionLabel.textContent = "Dimension Name";

    dimension = document.createElement("input");
    dimension.setAttribute('id', dimensionNameId);
    dimension.setAttribute('name', dimensionNameId);
    dimension.setAttribute('type', 'text');
    dimension.setAttribute('class', 'dimensionName');

    dataLocationLabel = document.createElement("label");
    dataLocationLabel.setAttribute('for', dataLocationNameId);
    dataLocationLabel.textContent = "Data Location"; // TO DO folder opener

    dataLocationEl = document.createElement("input");
    dataLocationEl.setAttribute('id', dataLocationNameId);
    dataLocationEl.setAttribute('name', dataLocationNameId);
    dataLocationEl.setAttribute('type', 'text');
    dataLocationEl.setAttribute('class', 'dataLocation');


    addFieldButton = document.createElement("input");
    addFieldButton.setAttribute('type', 'button');
    addFieldButton.setAttribute('onclick', 'addDimensionField(this)');
    addFieldButton.setAttribute('value', 'Add Field');

    fieldsDiv = document.createElement("div");
    fieldsDiv.setAttribute('id', divId + 'f');


    parent.appendChild(dimensionDiv)

    dimensionDiv.appendChild(dimensionLabel);
    dimensionDiv.appendChild(dimension);

    dimensionDiv.appendChild(dataLocationLabel);
    dimensionDiv.appendChild(dataLocationEl);


    dimensionDiv.appendChild(addFieldButton);
    dimensionDiv.appendChild(fieldsDiv);


}

const addDimensionField = (elRef) => {
    parentDivId = elRef.parentNode.id;

    fieldsDiv = document.getElementById(parentDivId + 'f');
    numFields = fieldsDiv.childElementCount;
    numFields += 1;

    fieldDivId = parentDivId + 'f' + numFields;
    isPKId = fieldDivId + '_isPK';
    nameId = fieldDivId + '_name';
    typeId = fieldDivId + '_type';
    isEntryPointId = fieldDivId + '_isEntryPoint';


    fieldDiv = document.createElement("div");
    fieldDiv.setAttribute('id', fieldDivId);
    fieldDiv.setAttribute('class', 'dimensionField');


    // PK
    isPKLabel = document.createElement("label");
    isPKLabel.setAttribute('for', isPKId);
    isPKLabel.textContent = "Is primary key?";

    isPKEl = document.createElement("input");
    isPKEl.setAttribute('id', isPKId);
    isPKEl.setAttribute('name', isPKId);
    isPKEl.setAttribute('type', 'checkbox');
    isPKEl.setAttribute('class', 'pk');


    // Name
    nameLabel = document.createElement("label");
    nameLabel.setAttribute('for', nameId);
    nameLabel.textContent = "Name";

    nameEl = document.createElement("input");
    nameEl.setAttribute('id', nameId);
    nameEl.setAttribute('name', nameId);
    nameEl.setAttribute('type', 'text');
    nameEl.setAttribute('class', 'name');



    // Type
    typeLabel = document.createElement("label");
    typeLabel.setAttribute('for', typeId);
    typeLabel.textContent = "Type";

    typeEl = document.createElement("input");
    typeEl.setAttribute('id', typeId);
    typeEl.setAttribute('name', typeId);
    typeEl.setAttribute('type', 'text');
    typeEl.setAttribute('class', 'type');

    // Entry Point
    isEntryPointLabel = document.createElement("label");
    isEntryPointLabel.setAttribute('for', isEntryPointId);
    isEntryPointLabel.textContent = "Is this an Entry Point for analysis?";

    isEntryPointEl = document.createElement("input");
    isEntryPointEl.setAttribute('id', isEntryPointId);
    isEntryPointEl.setAttribute('name', isEntryPointId);
    isEntryPointEl.setAttribute('type', 'checkbox');
    isEntryPointEl.setAttribute('class', 'entryPoint');


    fieldDiv.appendChild(isPKEl);
    fieldDiv.appendChild(isPKLabel);

    fieldDiv.appendChild(nameLabel);
    fieldDiv.appendChild(nameEl);

    fieldDiv.appendChild(typeLabel);
    fieldDiv.appendChild(typeEl);

    fieldDiv.appendChild(isEntryPointEl);
    fieldDiv.appendChild(isEntryPointLabel);

    fieldsDiv.appendChild(fieldDiv);

}

const addFTColumn = () => {
    parentDivId = "ft_columns";
    parentDiv = document.getElementById(parentDivId);

    numFactTableColumns += 1;
    columnDivId = parentDivId + numFactTableColumns;


    fkId = columnDivId + '_fk';
    nameId = columnDivId + '_name';
    typeId = columnDivId + '_type';


    ftColumnDiv = document.createElement("div");
    ftColumnDiv.setAttribute('id', columnDivId);
    ftColumnDiv.setAttribute('class', 'column');


    // PK
    fkLabel = document.createElement("label");
    fkLabel.setAttribute('for', fkId);
    fkLabel.textContent = "Foreign key";

    fkEl = document.createElement("input");
    fkEl.setAttribute('id', fkId);
    fkEl.setAttribute('name', fkId);
    fkEl.setAttribute('type', 'text'); // TO DO Dropdown from list of dimensions
    fkEl.setAttribute('class', 'fk');


    // Name
    nameLabel = document.createElement("label");
    nameLabel.setAttribute('for', nameId);
    nameLabel.textContent = "Name";

    nameEl = document.createElement("input");
    nameEl.setAttribute('id', nameId);
    nameEl.setAttribute('name', nameId);
    nameEl.setAttribute('type', 'text');
    nameEl.setAttribute('class', 'name');


    // Type
    typeLabel = document.createElement("label");
    typeLabel.setAttribute('for', typeId);
    typeLabel.textContent = "Type";

    typeEl = document.createElement("input");
    typeEl.setAttribute('id', typeId);
    typeEl.setAttribute('name', typeId);
    typeEl.setAttribute('type', 'text');
    typeEl.setAttribute('class', 'type');


    ftColumnDiv.appendChild(fkLabel);
    ftColumnDiv.appendChild(fkEl);

    ftColumnDiv.appendChild(nameLabel);
    ftColumnDiv.appendChild(nameEl);

    ftColumnDiv.appendChild(typeLabel);
    ftColumnDiv.appendChild(typeEl);

    parentDiv.appendChild(ftColumnDiv);

}

const toXML = () => {
    let xmldata = ['<?xml version="1.0"?>'];
    xmldata.push("<sdwh-schema>");

    let form = document.getElementById("schema_form");
    // let dimensions = document.getElementById("dimensions");
    // let ft_columns = document.getElementById("ft_columns");
    // let emitter_loc = document.getElementById("emitter_loc");
    // let aggregates = document.getElementById("aggregates");
    // let window_config = document.getElementById("window_config");


    // Dimensions
    xmldata.push("<dimensions>");
    $('#dimensions .dimension').each((i1, el1) => {
        let dimension = $(el1);

        let dimensionName = $(dimension.children(".dimensionName")[0]).val();
        let dataLocation = $(dimension.children(".dataLocation")[0]).val();
        xmldata.push('<dim name="' + dimensionName + '">');

        $(dimension.find(".dimensionField")).each((i3, e3) => {
            let dimensionField = $(e3);

            let isPk = $(dimensionField.children(".pk")[0]).is(":checked");
            let name = $(dimensionField.children(".name")[0]).val();
            let type = $(dimensionField.children(".type")[0]).val();
            let isEntryPoint = $(dimensionField.children(".entryPoint")[0]).is(":checked");

            xmldata.push('<field is-pk="' + isPk + '" is-EntryPoint="' + isEntryPoint + '">');
            xmldata.push('<name>' + name + '</name>');
            xmldata.push('<type>' + type + '</type>');
            xmldata.push('</field>');
        });
        xmldata.push('<data-loc>' + dataLocation + '</data-loc>');
        xmldata.push('</dim>');
    })
    xmldata.push("</dimensions>");


    // Fact table columns
    xmldata.push("<columns>");
    $('#ft_columns .column').each((i1, el1) => {
        let column = $(el1);

        let fk = $(column.children(".fk")[0]).val();
        let name = $(column.children(".name")[0]).val();
        let type = $(column.children(".type")[0]).val();

        xmldata.push('<column fk="' + fk + '">');
        xmldata.push('<name>' + name + '</name>');
        xmldata.push('<type>' + type + '</type>');
        xmldata.push('</column>');

    })
    xmldata.push("</columns>");


    // Emitter
    let emitterLocation = $("#emitter_loc input").val();
    xmldata.push("<fact-table>")
    xmldata.push('<loc>"' + emitterLocation + '"</loc>')
    xmldata.push("</fact-table>")


    // Aggregates
    xmldata.push("<aggregates>")
    $('#aggregates input').each((i, e) => {
        if ($(e).is(":checked")) xmldata.push('<agg>' + e.id + '</agg>')
    })
    xmldata.push("</aggregates>")

    // Window Config
    xmldata.push("<window-config>")
    xmldata.push('<window-size>' + $("#wsize").val() + '</window-size>')
    xmldata.push('<window-velocity>' + $("#wvelocity").val() + '</window-velocity>')
    // xmldata.push('<window-units>' + $("#wunits").val() + '</window-units>')
    xmldata.push("</window-config>")


    xmldata.push("</sdwh-schema>");

    let xmldoc = xmldata.join("\n")
    downloadString(xmldoc, "text/xml", "config_v2");
}

function downloadString(text, fileType, fileName) {
    let blob = new Blob([text], { type: fileType });

    let a = document.createElement('a');
    a.download = fileName;
    a.href = URL.createObjectURL(blob);
    a.dataset.downloadurl = [fileType, a.download, a.href].join(':');
    a.style.display = "none";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1500);
}

function redirect(htmlPage) {
    window.location.href = htmlPage;
}

function startEngine() {
    fetch('http://localhost:9001/execute')

        // Converting received data to JSON
        .then(response => response.json())
        .then(json => {
            console.log(json);
        });
    redirect("queryDisplay.html");
}

async function checkboxes() {
    console.log("Checkboxes api was called");
    var tickNumber;
    await fetch('http://localhost:9001/getTick') //TO FILL API URL HERE
        // Converting received data to JSON
        .then(response => response.json())
        .then(json => {
            tickNumber = json.tickNum;
        });
    console.log(tickNumber);
    fetch('http://localhost:9001/getEntryPoints') //TO FILL API URL HERE
        .then(response => response.json())
        .then(json => {
            console.log(json);
            let li = ``;
            // Loop through each data and add a checkbox
            json.EntryPoints.forEach(dim => {
                li += `<input type="checkbox" class="entryPoint" id=${dim} name=${dim}><label for=${dim}>${dim}</label>`;
            });
            li += `<br><input type="number" id="tick" name="tick" min="0" max=${tickNumber}><label for=tick>Select Tick Number</label>`
            // TO DO add input selector for window tick number 
            li += `<br><input type="button" value="Submit Query" onclick="queryApi()">`
            // Display result
            document.getElementById("entryPoints").innerHTML = li;
        });
}

function queryApi() {
    console.log("I was called");
    var content = [];
    var inputElements = document.getElementsByClassName('entryPoint');
    for (var i = 0; inputElements[i]; ++i) {
        if (inputElements[i].checked) {
            content.push(inputElements[i].id);
        }
    }
    var chooseTick = document.getElementById('tick');
    const body = { dimensions: content, tickNumber: chooseTick.value }
    fetch("http://localhost:9001/query", {
        method: "POST",
        // Adding body or contents to send
        body: JSON.stringify(body),
        // Adding headers to the request
        headers: {
            "Content-type": "application/json; charset=UTF-8"
        }
    })
        // Converting to JSON
        .then(response => response.json())

        // Displaying results to console
        .then(json => {
            console.log(json); //this is the result sent by the api. need to figure out how to display this in frontend
            const results = json.result;
            // const n_rows = results.length;
            const columns = Object.keys(results[0]);
            // const n_columns = columns.length;
            let li = "<tr>";
            columns.forEach(col => {
                li += `<th>${col}</th>`
            })
            li += "</tr>";
            results.forEach(row => {
                li += "<tr>";
                columns.forEach(column => {
                    li += `<td>${row[column]}</td>`
                })
                li += "</tr>";
            });

            // Display result
            document.getElementById("results").innerHTML = li;
        });
    // console.log(body);
}
