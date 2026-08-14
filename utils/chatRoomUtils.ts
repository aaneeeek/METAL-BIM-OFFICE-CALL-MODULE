import {types} from "mediasoup";
import {DefaultEventsMap, Server, Socket} from "socket.io";
import "dotenv/config";

const mediaCodecs: types.RouterRtpCodecCapability[] =
    [
        {
            kind        : "audio",
            mimeType    : "audio/opus",
            clockRate   : 48000,
            channels    : 2
        },
        {
            kind       : "video",
            mimeType   : "video/H264",
            clockRate  : 90000,
            parameters :
                {
                    "packetization-mode"      : 1,
                    "profile-level-id"        : "42e01f",
                    "level-asymmetry-allowed" : 1
                }
        }
    ];




export class callRoom {
    worker: types.Worker
    router: types.Router | undefined
    participants: {
        socketId: string;
        consumingTransport?: types.Transport;
        producingTransport?: types.Transport;
        producers: Map<string, types.Producer>;
    }[]

    constructor(worker: types.Worker) {
        this.participants = [];
        this.worker = worker;
    }

    async initialize(){
        this.router = await this.worker.createRouter({ mediaCodecs });
    }

    async connect(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>) {
        this.participants.push({ socketId: socket.id, producers: new Map() }); //save client socket

    }

    getRouterRTPCapabilities(){
        return this.router?.rtpCapabilities
    }

    async createTransports(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>){
        if (this.router){
            const producingTransport = await this.router.createWebRtcTransport({
                listenIps: [{ ip: '0.0.0.0', announcedIp: process.env.PUBLIC_IP||"127.0.0.1" }],
                enableUdp: true,
                enableTcp: true,
                preferUdp: true,
            });
            const consumingTransport = await this.router.createWebRtcTransport({
                listenIps: [{ ip: '0.0.0.0', announcedIp: process.env.PUBLIC_IP||"127.0.0.1" }],
                enableUdp: true,
                enableTcp: true,
                preferUdp: true,
            });
            const participant = this.participants.find(elt => elt.socketId === socket.id);
            if (participant) {
                participant.producingTransport = producingTransport;
                participant.consumingTransport = consumingTransport;
                return {
                    consumingTransportParams: {
                        id: consumingTransport.id,
                        iceParameters: consumingTransport.iceParameters,
                        iceCandidates: consumingTransport.iceCandidates,
                        dtlsParameters: consumingTransport.dtlsParameters,
                    },
                    producingTransportParams: {
                        id: producingTransport.id,
                        iceParameters: producingTransport.iceParameters,
                        iceCandidates: producingTransport.iceCandidates,
                        dtlsParameters: producingTransport.dtlsParameters,
                    }
                }
            }
            else{
                console.log(`Participant ${socket.id} not found. Could not create transport channels`);
                return null;
            }
        }
    }

    async connectSendTransport(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>, senderTransportDTLSParameters: types.DtlsParameters ){
        const participant = this.participants.find(elt => elt.socketId === socket.id);
        if (participant && participant.producingTransport) {
            await participant.producingTransport.connect({dtlsParameters: senderTransportDTLSParameters});
            return {message: "connected successfully."};
        }
        else{
            console.log(`Participant ${socket.id} not found. Could not connect transport channels`);
            return {message: `Could not connect sender transport successfully. Participant ${socket.id} not found`};
        }
    }

    async connectRecvTransport(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>, receiverTransportDTLSParameters: types.DtlsParameters ){
        const participant = this.participants.find(elt => elt.socketId === socket.id);
        if (participant) {
            await participant.consumingTransport?.connect({dtlsParameters: receiverTransportDTLSParameters});
            return {message: "connected successfully."};
        }
        else{
            console.log(`Participant ${socket.id} not found. Could not connect transport channels`);
            return {message: `Could not connect receiver transport successfully. Participant ${socket.id} not found`};
        }
    }

    async clientProduced(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>, {kind, rtpParameters, appData }: {kind: types.MediaKind, rtpParameters: types.RtpParameters, appData: types.AppData}){
        const participant = this.participants.find(elt => elt.socketId === socket.id);
        if (participant && participant.producingTransport) {
            const producer = await participant.producingTransport.produce({kind, rtpParameters, appData});
            participant.producers.set(producer.id, producer);
            await socket.broadcast.emitWithAck("newProducer", {producerId: producer.id});
            console.log("Propagated Producer Id", producer.id);
        }
        else{
            console.log("Matching transport not found");
        }
    }

    async consume(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>, { producerId, rtpCapabilities }: {producerId: string, rtpCapabilities: types.RtpCapabilities}){
        const participant = this.participants.find(elt => elt.socketId === socket.id);
        if (participant && this.router && this.router.canConsume({ producerId, rtpCapabilities })){
            if (participant.consumingTransport){
                const consumer = await participant.consumingTransport.consume({
                    producerId, rtpCapabilities, paused: false,
                });
                return {
                    id: consumer.id,
                    producerId,
                    kind: consumer.kind,
                    rtpParameters: consumer.rtpParameters,
                };
            }
            else{
                console.log(`Participant transport not found`);
                return {error: "Participant transport not found"};
            }
        }
        else{
            console.log("Error occurred, Could not find corresponding participant or router can not consume this media");
            return {error: "Error occurred, Could not find corresponding participant or router can not consume this media"}
        }


    }

}