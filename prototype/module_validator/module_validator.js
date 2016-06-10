
function ReportReceived(module, message) {
    console.log("RECEIVED at " + module.name + "\tContent: " + JSON.stringify(message));
}

function ReportPublished(module, message) {
    console.log("PUBLISHED by " + module.name + "\tContent: " + JSON.stringify(message));
}

var messageBus = {
    modules: [],
    AddToMessageBus: function (module) {
        this.modules.push(module);
    },
    Publish: function (module, message) {
        ReportPublished(module, message);
        for (var index = 0; index < this.modules.length; index++) {
            var element = this.modules[index];
            if (module != element) {

                element.Receive(message);
            }
        }
    }
};

var moduleList = [
    {
        name: 'IoTHub',
        Receive: function(message) {
            if ((message.dest === this.name) &&
                ('iotId' in message) &&
                ('iotKey' in message)) {

                    ReportReceived(this, message);
            }
        }
    },
    {
        name: 'Router',
        Receive: function(message) {
            if (((!('dest' in message)) || (message.dest === 'IoTHub')) && 
                ('deviceId' in message) &&
                ('deviceKey' in message)) {

                    ReportReceived(this, message);

                    var newMessage = JSON.parse(JSON.stringify(message)); 
                    newMessage.dest = 'IoTHub';
                    newMessage.iotId = 'IoTHub';
                    newMessage.iotKey = 'IoTHub';
                    delete newMessage.deviceId;
                    delete newMessage.deviceKey;
                    
                    messageBus.Publish(this, newMessage);
            }
        }
    },
    {
        name: 'IDMAP',
        Receive: function(message) {
            if ('deviceId' in message) {
                ReportReceived(this, message);
                
                var newMessage = JSON.parse(JSON.stringify(message));
                delete newMessage.deviceId;
                newMessage.macAddress = '01:01';
                messageBus.Publish(this, newMessage);

            }
            if ('macAddress' in message) {

                ReportReceived(this, message);

                var newMessage = JSON.parse(JSON.stringify(message));
                delete newMessage.macAddress;
                newMessage.deviceId = 'name';
                newMessage.deviceKey = 'key';
                messageBus.Publish(this, newMessage);
            }

        }
    },
    {
        name: 'SIM_BLE',
        Receive: function(message) {
            if (('uuid' in message) &&
                ('macAddress' in message)) {

                ReportReceived(this, message);

                var newMessage = new Object({
                    src: 'BLE',
                    macAddress: message.macAddress,
                    uuid: 'UUID'
                })
                if (message.src)
                {
                    newMessage.dest = message.src;
                }
                messageBus.Publish(this, newMessage);
                }
        }
    },
    { 
        name: 'DMHUB',
        Receive: function(message) {
            if ((message.dest === this.name) &&
                ('deviceId' in message) &&
                ('deviceKey' in message)) {

                    ReportReceived(this, message)                    
                }
        }
    },
    {
        name:'DMWORKER',
        Receive: function(message) {
            if ( (message.dest === this.name) &&
                 ('deviceId' in message) &&
                 ('deviceKey' in message)) {

                    ReportReceived(this, message)

                    var newMessage = JSON.parse(JSON.stringify(message));
                    newMessage.dest = 'DMHUB';
                    newMessage.src = 'DMWORKER';
                    delete newMessage.uuid;

                    messageBus.Publish(this, newMessage);
                  
                 }
            if ( (message.dest === 'BLE') &&
                 ('request' in message) &&
                 ('dmId' in message)) { 
                    
                    ReportReceived(this, message)

                    var newMessage = JSON.parse(JSON.stringify(message));
                    newMessage.dest = 'BLE';
                    newMessage.src = 'DMWORKER';
                    newMessage.deviceId = message.dmId;
                    newMessage.uuid = 'UUID';
                    delete newMessage.request;
                    delete newMessage.dmId;


                    messageBus.Publish(this, newMessage);
                }
        }
    }
]

function main() {
    var moduleHash = {};
    for (var index = 0; index < moduleList.length; index++) {
        var m = moduleList[index];
        
        messageBus.AddToMessageBus(m);
        moduleHash[m.name] = m;
    }

    //tests
    
    // DM path

    console.log("DM path\n");
    messageBus.Publish(moduleHash['DMHUB'], new Object({
        dest: 'BLE',
        dmId: 'name',
        src: 'DMHUB',
        request: 'a DM request'
    }));


    // IoTHub D2C
    console.log("\nIoTHub D2C path\n");
    messageBus.Publish(moduleHash['SIM_BLE'], new Object({
        src: 'BLE',
        macAddress: '01:01',
        uuid: 'UUID'
    }))

    // IoTHub C2D
    console.log("\nIoTHub C2D path\n");
    messageBus.Publish(moduleHash['IoTHub'], new Object({
        src: 'IoTHub',
        dest: 'device',
        uuid: 'UUID',
        deviceId: 'a device name',
    }))

}

main()