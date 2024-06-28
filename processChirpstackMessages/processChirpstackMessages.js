/**
 * Type: Stream Service
 * Description: A service that does not have an execution timeout which allows for infinite execution of logic.
 * @param {CbServer.BasicReq} req
 * @param {string} req.systemKey
 * @param {string} req.systemSecret
 * @param {string} req.userEmail
 * @param {string} req.userid
 * @param {string} req.userToken
 * @param {boolean} req.isLogging
 * @param {[id: string]} req.params
 * @param {CbServer.Resp} resp
 */

function processChirpstackMessages(req, resp) {

    ClearBlade.init({ request: req });

    const payload = {
      loraAddr: "172.31.0.1",
      loraPort: 1883
    }

    var edgeName = ClearBlade.edgeId();
    var count = 0;

    log("service starting")

    var radLoRaOptions = {
        address: payload.loraAddr,
        port: payload.loraPort
    };

    //the client that connects to chirpstack
    var loraClient;
    try {
        loraClient = new MQTT.Client(radLoRaOptions);
        log("connected to lora client")
    } catch (e) {
        resp.error("failed to init lora client: " + e);
    }

    //log("creating cb client");
    var cbClient;
    try {
        cbClient = new MQTT.Client();
    } catch (e) {
        resp.error("failed to init cb client: " + e);
    }

    const LORA_UPLINK_TOPIC = "application/+/device/+/event/up" // TODO: Replace with actual topic for Device Provisioning

    loraClient.subscribe(LORA_UPLINK_TOPIC, function (topic, msg) {
        log("topic: " + topic);
        //        log("message: " + JSON.stringify(msg));
        processMessage(msg, topic);
    });

    function hexToDec(hexString) {
        return parseInt(hexString, 16);
    }

    function hex_to_ascii(str1) {
        var hex = str1.toString();
        var str = '';
        for (var n = 0; n < hex.length; n += 2) {
            str += String.fromCharCode(parseInt(hex.substr(n, 2), 16));
        }
        return str;
    }

    function processMessage(msg) {
        try {
            var payload = new TextDecoder("utf-8").decode(msg.payload_bytes);
            log("message")
            log(JSON.stringify(payload))
            msg = JSON.parse(payload);
            log("keys are " + Object.keys(msg))
            log("deveui is " + msg.devEui)
            var device = msg.deviceInfo.devEui
            // log("device data is: ", msg.data)
            log("DEVICE is: ", device)
            var keys = Object.keys(msg)


            var assetUpdatMessage = {
                        id: msg.deviceInfo.devEui, //ID Of the unique asset
                        type: msg.deviceInfo.deviceName,  //Type of Asset to update, ex: "EM-300-TH"
                        custom_data: {
                            Reporting: true
                        }

                    }

            var attributes = Object.keys(msg.object) //this field contains sensor data
            for(x=0; x < attributes.length; x++){
                assetUpdatMessage.custom_data[attributes[x]] = msg.object[attributes[x]]
            }

            // *********** PUBLISH ***********
            log("Publishing this: ", JSON.stringify(assetUpdatMessage));
            cbClient.publish("_monitor/asset/default/data/_platform", JSON.stringify(assetUpdatMessage))

        }
        catch (e) {
            log("failed to parse json: " + e);
        }
    }

    function hexToBytes(hex) {
        var bytes = [];
        for (var c = 0; c < hex.length; c += 2) {
            bytes.push(parseInt(hex.substr(c, 2), 16));
        }
        return bytes;
    }

    function decodeMessage(bytes) {

        var fields = {};
        var result = {};

        // Message must be at least 1 byte so that we can decode the Protocol and Message Type.
        // All other fields depend on these two.
        if (bytes.length < 1) {
            throw new Error("Payload is empty");
        }

        // Protocol (4-Bits) -> 4: Protocol D; Other: Unsupported
        fields.Protocol = ((bytes[0] & 0xf0) >> 4);
        result.Protocol = String.fromCharCode("A".charCodeAt(0) - 1 + fields.Protocol);
        if (fields.Protocol == 4) {
            log("Protocol is 4")
            result = decodeProtD(bytes);
        }
        //       else {
        //            log("Protocol is not 4")
        //            log("Protocol is: ", fields.Protocol);
        //            throw new Error("Unsupported Protocol " + fields.Protocol + "(" + result.Protocol + ")");
        //        }

        //        log("result " + JSON.stringify(result))
        return (result);
    }

    function decodeProtD(bytes) {

        var fields = {};
        var result = {};

        // Message must be at least 1 byte so that we can decode the Protocol and Message Type.
        // All other fields depend on these two.
        if (bytes.length < 1) {
            throw new Error("Payload is empty");
        }

        // Message Type (4-Bits) -> 0: INFO; 1: READING
        fields.Msg_type = (bytes[0] & 0x0f);
        if (fields.Msg_type == 0) {
            result.Msg_type = "INFO";
            //result = {...result, ...decodeProtDInfo(bytes)};
        }
        else if (fields.Msg_type == 1) {
            result = decodeProtDReading(bytes);
            result.Msg_type = "READING";
        }
        else if (fields.Msg_type == 2) {
            result.Msg_type = "DEBUG";
            //result = {...result, ...decodeProtDDebug(bytes)};
        }
        else if (fields.Msg_type == 3) {
            result.Msg_type = "CONFIG";
            //result = {...result, ...decodeProtDConfig(bytes)};
        }
        else {
            throw new Error("Unknown Message Type " + fields.Msg_type);
        }

        return (result);
    }

    function decodeProtDReading(bytes) {

        var fields = {};
        var result = {};

        // Message length for Protocol D READING message shold be 19 bytes
        if (bytes.length != 19) {
            throw new Error("Invalid payload size");
        }

        // Message ID (8-Bits) -> Sequencial Number (0 upon restart; rotates at 127)
        fields.msg_ID = (bytes[1]);
        result.msg_ID = fields.msg_ID;

        // Reset Indicator (2-Bits) -> 2 LSbits of the Reset Counter
        fields.Reset_Ind = ((bytes[2] & 0xc0) >> 6);
        result.Reset_Ind = fields.Reset_Ind;

        // Operating Mode (2-Bits) -> 0: INACTIVE; 1: ACTIVE; 2: RFU; 3: ERROR
        fields.Op_Mode = ((bytes[2] & 0x30) >> 4);
        if (fields.Op_Mode == 0) { result.Op_Mode = "INACTIVE"; }
        else if (fields.Op_Mode == 1) { result.Op_Mode = "ACTIVE"; }
        else if (fields.Op_Mode == 2) { result.Op_Mode = "RFU"; }
        else if (fields.Op_Mode == 3) { result.Op_Mode = "ERROR"; }
        else { result.Op_Mode = "ERROR"; }

        // Message Reason (4-Bits) -> 2: SCHEDULE
        fields.msg_reas = (bytes[2] & 0x0f);
        if (fields.msg_reas == 0) { result.msg_reas = "UNK"; }
        else if (fields.msg_reas == 1) { result.msg_reas = "INIT"; }
        else if (fields.msg_reas == 2) { result.msg_reas = "SCHEDULE"; }
        else if (fields.msg_reas == 3) { result.msg_reas = "EVENT"; }
        else if (fields.msg_reas == 4) { result.msg_reas = "ALERT"; }
        else if (fields.msg_reas == 5) { result.msg_reas = "RFU"; }
        else if (fields.msg_reas == 6) { result.msg_reas = "USER REQ (LOCAL)"; }
        else if (fields.msg_reas == 7) { result.msg_reas = "USER REQ (OTA)"; }
        else if (fields.msg_reas == 8) { result.msg_reas = "USER REQ (BLE)"; }
        else if (fields.msg_reas == 9) { result.msg_reas = "TEST"; }
        else { result.msg_reas = "ERROR"; }

        // Seconds since last restart (32-Bits) - BIG ENDIAN
        // NOTE: The top bit being multiplied by (1 << 24) instead of directly being shifted is a trick to prevent Javascript from making it a negative in case the top bit is set.
        fields.t_restart = ((bytes[3] * (1 << 24)) + (bytes[4] << 16) + (bytes[5] << 8) + (bytes[6]));
        result.t_restart = fields.t_restart;

        // Restart Reason (4-Bits) -> 1: Power Up
        fields.r_reas = ((bytes[7] & 0xf0) >> 4);
        if (fields.r_reas == 0) { result.r_reas = "UNK"; }
        else if (fields.r_reas == 1) { result.r_reas = "POWER UP"; }
        else if (fields.r_reas == 2) { result.r_reas = "POWER UP (BB)"; }
        else if (fields.r_reas == 3) { result.r_reas = "PIN"; }
        else if (fields.r_reas == 4) { result.r_reas = "LOCKUP"; }
        else if (fields.r_reas == 5) { result.r_reas = "SYSTEM"; }
        else if (fields.r_reas == 6) { result.r_reas = "WATCHDOG"; }
        else if (fields.r_reas == 7) { result.r_reas = "CONTROLLED"; }
        else if (fields.r_reas == 8) { result.r_reas = "FW UPGRADE"; }
        else { result.r_reas = "ERROR"; }

        // WakeUp Reason (4-Bits) -> 2: Schedule/RTC
        fields.w_reas = (bytes[7] & 0x0f);
        if (fields.w_reas == 0) { result.w_reas = "UNK"; }
        else if (fields.w_reas == 1) { result.w_reas = "INIT"; }
        else if (fields.w_reas == 2) { result.w_reas = "RTC"; }
        else if (fields.w_reas == 3) { result.w_reas = "SERIAL"; }
        else if (fields.w_reas == 4) { result.w_reas = "NFC"; }
        else if (fields.w_reas == 5) { result.w_reas = "BLE"; }
        else if (fields.w_reas == 6) { result.w_reas = "ACC"; }
        else if (fields.w_reas == 7) { result.w_reas = "RFU"; }
        else if (fields.w_reas == 8) { result.w_reas = "RFU"; }
        else { result.w_reas = "ERROR"; }

        // Battery Voltage (8-Bits) -> in 0.1 V (0 - 25 V) values above 250 reserved
        fields.bat_V = (bytes[8]);
        if (fields.bat_V <= 250) {
            result.bat_V_valid = true;
            result.bat_V = fields.bat_V * 0.1;
        }
        else {
            result.bat_V_valid = false;
            // I am not including the value given it is invalid!  Feel free to include a special code if you feel it is better!
        }

        // Reading Result (4-Bits) -> 1: OK; 2: NO ECHO; 3: ABORTED; 4: HW ERROR; 7: ERROR; Else: RFU
        fields.read_res = ((bytes[9] & 0xf0) >> 4);
        if (fields.read_res == 1) { result.read_res = "OK"; }
        else if (fields.read_res == 2) { result.read_res = "NO ECHO"; }
        else if (fields.read_res == 3) { result.read_res = "ABORTED"; }
        else if (fields.read_res == 4) { result.read_res = "HW ERROR"; }
        else if (fields.read_res == 7) { result.read_res = "ERROR"; }
        else { result.read_res = "RFU"; }

        // Sensor Type (4-Bits) -> 3: 150 KHz (up to 10 ft)
        fields.Sens_Type = (bytes[9] & 0x0f);
        if (fields.Sens_Type == 0) { result.Sens_Type = "NONE"; }
        else if (fields.Sens_Type == 2) { result.Sens_Type = "US 150 KHz"; }
        else if (fields.Sens_Type == 3) { result.Sens_Type = "US 150 KHz"; }
        else if (fields.Sens_Type == 8) { result.Sens_Type = "US 95 KHz"; }
        else if (fields.Sens_Type == 9) { result.Sens_Type = "US 95 KHz"; }
        else { result.Sens_Type = "UNK"; }

        // Distance (16-Bits) -> in mm (0 - 65000 V) values above 65000 represent error codes - BIG ENDIAN
        fields.Distance = ((bytes[10] << 8) + (bytes[11]));
        if (fields.Distance < 65000) {
            result.Distance_valid = true;
            result.Distance = fields.Distance / 1000.0; // Converted to meters
            result.Distance_code = 0; // No Error
        }
        else {
            result.Distance_valid = false;
            // I am not including the value given it is invalid!  Feel free to include a special code if you feel it is better!
            result.Distance_code = fields.Distance; // Error code kept AS-IS!
        }

        // Temp (8-Bits) -> 0.5 *C (0 = -60 to 239 = 59.5); 240 and above reserved
        fields.Temp = (bytes[12]);
        if (fields.Temp < 240) {
            result.Temp_valid = true;
            result.Temp = ((fields.Temp * 0.5) - 60.0); // Converted to *C
        }
        else {
            result.Temp_valid = false;
            // I am not including the value given it is invalid!  Feel free to include a special code if you feel it is better!
        }

        // Tilt (8-Bits) -> 0.1* - 0 to 25.4; 0xff Error Code
        fields.Tilt = (bytes[13]);
        if (fields.Tilt < 254) {
            result.Tilt_valid = true;
            result.Tilt = (fields.Tilt * 0.1); // Converted to *
        }
        else {
            result.Tilt_valid = false;
            // I am not including the value given it is invalid!  Feel free to include a special code if you feel it is better!
        }

        // Event Avg. Energy (8-Bits) -> Average Energy from the start to end of event
        fields.Avg_Energy = (bytes[14]);
        result.Avg_Energy = fields.Avg_Energy;

        // Background Noise (4-Bits) -> 2 LSbits of the Reset Counter
        fields.Bkgnd_Noise = ((bytes[15] & 0xf0) >> 4);
        result.Bkgnd_Noise = fields.Bkgnd_Noise;

        // Successfull Attempt (4-Bits) -> 1st Attempt Found
        fields.Attempt = (bytes[15] & 0x0f);
        result.Attempt = fields.Attempt;

        // Sens_Health (3-Bit) -> Sensor health (0-7)
        fields.Sens_Health = ((bytes[16] & 0x70) >> 4);
        result.Sens_Health = fields.Sens_Health;

        // Reserved (12-Bits) -> RFU
        fields.Eng = (((bytes[16] & 0x0f) << 8) + (bytes[17]));
        result.Eng = fields.Eng;

        // Message CRC (8-Bits) -> CRC
        fields.Checksum = bytes[18];
        result.Checksum = fields.Checksum;

        return (result);
    }

    // This is the decoder for the Milesight Temperature and RH bytes
    // Basically its a byte swap where the new high byte gets multiplied by
    // 256 --- little endian
    function readUInt16LE(bytes) {
        var value = (bytes[1] << 8) + bytes[0];
        return value & 0xffff;
    }

}
