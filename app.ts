import express from 'express';
import cors from "cors";
import https from "https";
import "dotenv/config";
import {Server} from "socket.io";
import workers from "./utils/mediasoup-worker";
import {callRooms} from "./utils/global_vars";
import {callRoom} from "./utils/chatRoomUtils";
import {connect} from "./utils/websocket-controller";
import {types} from "mediasoup";
import * as fs from "node:fs";


const options = {
    key: fs.readFileSync("/app/certs/key.pem"),
    cert: fs.readFileSync("/app/certs/cert.pem"),
};
const app = express();
let serverWorkers: types.Worker[] = []; // worker processes running on server


// add allowed origin cors
app.use(cors({
    origin: process.env.UI_BOUNDARY,
    methods: ["GET", "POST", "PUT", "DELETE"],
    credentials: true
}));
app.use(express.json());
app.post("/call/create", async (req, res) => {
    const {callId}: {callId: string} = req.body;
    const selectedWorkerIndex = 0; //an algorithm to select the worker to be used
    const worker = serverWorkers[selectedWorkerIndex];
    if (worker) {
        const callObject = new callRoom(worker, callId)
        await callObject.initialize(); // creating chat router
        callRooms.set(callId, callObject);
        console.log("call creation successful");
        res.json({
            message: "call creation successful",
            callId
        });
    }
    else{
        res.json({message: "call creation failed. Could not find ready worker"});
    }
})

const server = https.createServer(options,app);
const io = new Server(server, {
    cors: {
        origin: process.env.UI_BOUNDARY,
        methods: ["GET", "POST"],
        credentials: true
    }
});

// Websocket events
io.on("connection", async (socket)=>{
    const roomId = socket.handshake.auth.roomId;
    if (roomId){
        console.log(roomId);
        const callObject = callRooms.get(roomId);
        if (callObject){
            await callObject.connect(socket);
            socket.join(roomId);
            //client socket events
            socket.on('getRouterRTPCapabilities', (callBack)=>{
                callBack(callObject.getRouterRTPCapabilities());
            });

            socket.on('requestTransportCreation', async(callBack)=>{
                console.log("requestTransportCreation")
                const transportParams = await callObject.createTransports(socket);
                callBack(transportParams); // collect on the client side
            });

            socket.on('connectSenderTransport', async (DTLSParameters, callBack)=>{
                console.log("connectSenderTransport")
                const result = await callObject.connectSendTransport(socket, DTLSParameters);
                callBack(result);
            });

            socket.on('connectRecvTransport', async (DTLSParameters, callBack)=>{
                console.log("connectRecvTransport")
                const result = await callObject.connectRecvTransport(socket, DTLSParameters);
                callBack(result);
            });

            socket.on('transportProduce', async ({ kind, rtpParameters, appData }, callBack)=>{
                console.log("transportProduce");
                const result = await callObject.clientProduced(socket, {kind, rtpParameters, appData});
                callBack(result);
            });

            socket.on('clientConsume', async ({ producerId, rtpCapabilities }, callBack, producerSocketId)=>{
                console.log("clientConsume");
                const consumptionParams = await callObject.consume(socket, {producerId, rtpCapabilities});
                callBack(consumptionParams);
            });

            socket.on("getExistingProducers", async()=>{
                await callObject.configureNewSockets(socket);
            });

            socket.on("message", (message)=>{
                socket.to(roomId).emit("message", message, socket.id);
            });

            socket.on("disconnect", ()=>{
                const index = callObject.participants.findIndex(elt=> elt.socketId === socket.id);
                if (index && index >= 0) {
                    callObject.participants.splice(index, 1);
                    io.to(roomId).emit("socketdisconnected", socket.id);
                }
            })
        }
        else{
            console.error("Room not found");
        }
    }
    else{
        console.log("Invalid room id")
    }

});

(async()=>{
    serverWorkers = await workers;
    // if client request upgrade to websocket
    server.on("upgrade", (request, socket, head) => {
        const {url} = request;
        console.log("upgrade requested at ", url)
    });


    server.listen(443, async () => {
        console.log("Server running on port 443 with allowed cors origin ", process.env.UI_BOUNDARY);
    });
})()

