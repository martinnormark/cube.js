const azTables = require('./driver/AzureTablesDriver');

const tt = new azTables({});


tt.testConnection()
    .then(res => {
        if (res === true) {
            console.log('Connection is working...');

            // tt.query('PartitionKey eq ?', ['PullRequest', 'NACAS-Shamrock']).then(res => {
            //     console.log(res);
            // });

            // const token = tt.getSasToken('PullRequest').then(tok => console.log('token', tok));

            // console.log(token);

            // var tables = tt.getTables().then(tables => {
            //     return tables;
            // });

            //console.log(tables);

            var schema = tt.query(tt.informationSchemaQuery()).then(sc => {
                console.log(sc);
                return sc;
            });
        }
    });
