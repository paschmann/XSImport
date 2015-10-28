try {
    var conn = $.db.getConnection();
    var filename = $.request.parameters.get("filename");
    var schemaname = $.request.parameters.get("schemaname");
    var tablename = $.request.parameters.get("tablename");
    var proc = $.request.parameters.get("process");
    var emptyisnull = $.request.parameters.get("nulls");
    var delrows = $.request.parameters.get("delete");
    var csvpreviewrow = $.request.parameters.get("csvpreviewrow");

    var contents = "";
    var html = "";
    var response = {};
    var messages = [];

    if (proc === "preview") {
        contents = $.request.body.asString();
        previewFile();
    } else if (proc === "upload") {
        contents = $.request.body.asString();
        uploadFile();
    } else if (proc === "delete") {
        deleteTableData();
    } else if (proc === "init") {
        getSchemas();
        getTables();
    }
    $.response.status = $.net.http.OK;
} catch (err) {
    messages.push(err.message);
    $.response.status = 400;
} finally {
    response.messages = messages;
    $.response.contentType = "text/html";
    $.response.setBody(JSON.stringify(response));
}

function getSchemas() {
    var hdbconn = $.hdb.getConnection();
    var query = 'SELECT SCHEMA_NAME FROM "SYS"."SCHEMAS" WHERE HAS_PRIVILEGES = \'TRUE\'';
    var rs = hdbconn.executeQuery(query);
    response.schemas = rs;
}

function getTables() {
    var hdbconn = $.hdb.getConnection();
    var query = 'SELECT SCHEMA_NAME, TABLE_NAME FROM "SYS"."TABLES" ORDER BY TABLE_NAME';
    var rs = hdbconn.executeQuery(query);
    response.tables = rs;
}

function deleteTableData() {
    try {
        var pstmt1 = conn.prepareStatement("DELETE FROM " + schemaname + "." + tablename);
        var rs1 = pstmt1.executeQuery();
        pstmt1.close();
        conn.commit();
        messages.push("All rows deleted from " + schemaname + "." + tablename + "</br />");
    } catch (err) {
        messages.push(err.message);
    }
}

function parseTimestamp(strDate) {
    var year = strDate.substring(0, 4);
    var month = strDate.substring(4, 6);
    var day = strDate.substring(6, 8);
    var hour = strDate.substring(8, 10);
    var minute = strDate.substring(10, 12);
    var second = strDate.substring(12, 14);

    return new Date(year, month - 1, day, hour, minute, second);
}

function checkForBadData(arrLines) {
    for (var i = 0; i < arrLines.length; i++) {
        if (JSON.stringify(arrLines[i]).length <= 2) {
            arrLines.splice(i, 1);
            checkForBadData(arrLines);
        }
    }
    return arrLines;
}

function previewFile() {
    
    var pstmt = conn.prepareStatement('SELECT * FROM "' + schemaname + '"."' + tablename + '" LIMIT 1');
    var rs = pstmt.executeQuery();
    var rsm = rs.getMetaData();
    var colCount = rsm.getColumnCount();
    var rowData = [];
    var tableData = 0;

    if (rs.next()) {
        tableData = 1;
    }
    if (contents.length > 0) {
        var arrLines = contents.split(/\r\n|\n/);

        arrLines = checkForBadData(arrLines);
        
        var line = arrLines[csvpreviewrow];
        line = line.split("\",\"");
        var col = line.splice(0, colCount);
        for (var a = 1; a <= colCount; a++) {
            var row = {};
            var colType = "";

            switch (rsm.getColumnType(a)) {
                case $.db.types.VARCHAR:
                case $.db.types.CHAR:
                    colType = "VARCHAR/CHAR";
                    break;
                case $.db.types.NVARCHAR:
                case $.db.types.NCHAR:
                case $.db.types.SHORTTEXT:
                    colType = "NVARCHAR/NCHAR/SHORTTEXT";
                    break;
                case $.db.types.TINYINT:
                case $.db.types.SMALLINT:
                case $.db.types.INT:
                case $.db.types.BIGINT:
                    colType = "TINYINT/SMALLINT/INT/BIGINT";
                    break;
                case $.db.types.DOUBLE:
                    colType = "DOUBLE";
                    break;
                case $.db.types.DECIMAL:
                    colType = "DECIMAL";
                    break;
                case $.db.types.REAL:
                    colType = "REAL";
                    break;
                case $.db.types.NCLOB:
                case $.db.types.TEXT:
                    colType = "TEXT/NCLOB";
                    break;
                case $.db.types.CLOB:
                    colType = "CLOB";
                    break;
                case $.db.types.BLOB:
                    colType = "BLOB";
                    break;
                case $.db.types.DATE:
                    colType = "DATE";
                    break;
                case $.db.types.TIME:
                    colType = "TIME";
                    break;
                case $.db.types.TIMESTAMP:
                    colType = "TIMESTAMP";
                    break;
                case $.db.types.SECONDDATE:
                    colType = "SECONDDATE";
                    break;
                default:
                    colType = "STRING/DEFAULT";
            }

            var val = "";
            if (typeof col[a - 1] === 'undefined') {
                val = "";
            } else {
                val = col[a - 1].split("\"").join("");
                val = val.replace("\\,", ",");
            }

            if (typeof val === 'undefined' || (val === "" && emptyisnull === 'on')) {
                row.value = "null";
            } else {
                row.value = val;
            }

            row.columnname = rsm.getColumnLabel(a);
            row.columndatatype = rsm.getColumnTypeName(a);
            row.columnprecision = rsm.getPrecision(a);
            row.columnscale = rsm.getScale(a);
            row.columntype = colType;
            if (tableData === 1) {
                row.columndata = rs.getString(a);
            } else {
                row.columndata = "No Data";
            }
            rowData.push(row);
        }
        response.tabledata = rowData;
        messages.push("Preview generated.");
    } else {
        messages.push("No data in the submitted file.");
    }
    return html;
}

function uploadFile() {
    try {
        //Query Tabe metadata and get the content type of each column
        var pstmt = conn.prepareStatement("SELECT * FROM " + schemaname + "." + tablename + " LIMIT 1");
        var rs = pstmt.executeQuery();
        var rsm = rs.getMetaData();
        var colCount = rsm.getColumnCount();
        var startdt = Date.now();

        if (contents.length > 0) {
            var arrLines = contents.split(/\r\n|\n/);
            var placeholder = new Array(colCount + 1).join('?').split('').join(',');

            var insertStmnt = "INSERT INTO " + schemaname + "." + tablename + " VALUES (" + placeholder + ")";
            pstmt = conn.prepareStatement(insertStmnt);

            arrLines = checkForBadData(arrLines);

            pstmt.setBatchSize(arrLines.length);

            if (delrows === "on") {
                deleteTable();
            }

            for (var i = 0; i < arrLines.length; i++) {
                var line = arrLines[i].split("\",\"");
                var col = line.splice(0, arrLines.length + 1);
                if (JSON.stringify(arrLines[i]).length > 2) {
                    for (var a = 1; a <= colCount; a++) {
                        var val = "";
                        if (typeof col[a - 1] === 'undefined') {
                            val = "";
                        } else {
                            val = col[a - 1].split("\"").join("");
                            val = val.replace("\\,", ",");
                        }
                        if (typeof val === "undefined" || (val === "" && emptyisnull === "on")) {
                            pstmt.setNull(a);
                        } else {
                            switch (rsm.getColumnType(a)) {
                                case $.db.types.VARCHAR:
                                case $.db.types.CHAR:
                                    pstmt.setString(a, val);
                                    break;
                                case $.db.types.NVARCHAR:
                                case $.db.types.NCHAR:
                                case $.db.types.SHORTTEXT:
                                    pstmt.setNString(a, val);
                                    break;
                                case $.db.types.TINYINT:
                                case $.db.types.SMALLINT:
                                case $.db.types.INT:
                                case $.db.types.BIGINT:
                                    pstmt.setInteger(a, parseInt(val));
                                    break;
                                case $.db.types.DOUBLE:
                                    pstmt.setDouble(a, val);
                                    break;
                                case $.db.types.DECIMAL:
                                    pstmt.setDecimal(a, val);
                                    break;
                                case $.db.types.REAL:
                                    pstmt.setReal(a, val);
                                    break;
                                case $.db.types.NCLOB:
                                case $.db.types.TEXT:
                                    pstmt.setNClob(a, val);
                                    break;
                                case $.db.types.CLOB:
                                    pstmt.setClob(a, val);
                                    break;
                                case $.db.types.BLOB:
                                    pstmt.setBlob(a, val);
                                    break;
                                case $.db.types.DATE:
                                    pstmt.setDate(a, val);
                                    break;
                                case $.db.types.TIME:
                                    pstmt.setTime(a, val);
                                    break;
                                case $.db.types.TIMESTAMP:
                                    pstmt.setTimestamp(a, parseTimestamp(val)); //20140522180910526000
                                    break;
                                case $.db.types.SECONDDATE:
                                    pstmt.setSeconddate(a, val);
                                    break;
                                default:
                                    pstmt.setString(a, val);
                                    break;
                            }
                        }
                    }
                    pstmt.addBatch();
                }
            }
            pstmt.executeBatch();
            messages.push(arrLines.length + " Lines inserted into " + schemaname + "." + tablename + "<br />");
            messages.push(((Date.now() - startdt) / 60).toFixed(2) + " Seconds taken to complete<br />");
        } else {
            messages.push("No data in the submitted file.");
        }
    } catch (err) {
        messages.push(err.message + ": Error on file line - " + i + " in column #" + a + ". Running the preview function using this number might help to find the issue.");
    } finally {
        pstmt.close();
        conn.commit();
        conn.close();
    }
}
