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
    roomId: string;
    worker: types.Worker
    router: types.Router | undefined
    participants: {
        socketId: string;
        consumingTransport?:  types.WebRtcTransport<types.AppData>;
        producingTransport?:  types.WebRtcTransport<types.AppData>;
        producers: Map<string, types.Producer>;
        consumers: Map<string, types.Consumer>
    }[]

    constructor(worker: types.Worker, roomId: string) {
        this.participants = [];
        this.worker = worker;
        this.roomId = roomId;
    }

    async initialize(){
        this.router = await this.worker.createRouter({ mediaCodecs });
    }

    async configureNewSockets(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>){
        // this method is to make a new connection entering the chat/call room to catch up with the others
        // this method can only be called after the receiver transports for a socket are already set and connected
        console.log("A new device joined -- configuration started")
        for (const participant of this.participants) {
            const index = this.participants.indexOf(participant);
            if (participant.socketId !== socket.id){
                for (const [key, value] of participant.producers){
                    console.log("found new producer")
                    socket.emit("newProducer", {producerId: key, socketId: participant.socketId});
                }
            }
        }
    }

    async connect(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>) {
        this.participants.push({ socketId: socket.id, producers: new Map(), consumers: new Map() }); //save client socket
    }

    getRouterRTPCapabilities(){
        return this.router?.rtpCapabilities
    }

    async createTransports(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>){
        if (this.router){
            const producingTransport = await this.router.createWebRtcTransport({
                listenInfos: [
                    {
                        protocol: 'udp',
                        ip: '0.0.0.0',
                        announcedAddress: process.env.PUBLIC_IP!,
                        portRange: {
                            min: 40000,
                            max: 40100,
                        },
                    },
                    {
                        protocol: 'tcp',
                        ip: '0.0.0.0',
                        announcedAddress: process.env.PUBLIC_IP!,
                        portRange: {
                            min: 40101,
                            max: 40200,
                        },
                    },
                ],
            });
            const consumingTransport = await this.router.createWebRtcTransport({
                listenInfos: [
                    {
                        protocol: 'udp',
                        ip: '0.0.0.0',
                        announcedAddress: process.env.PUBLIC_IP!,
                        portRange: {
                            min: 40000,
                            max: 40100,
                        },
                    },
                    {
                        protocol: 'tcp',
                        ip: '0.0.0.0',
                        announcedAddress: process.env.PUBLIC_IP!,
                        portRange: {
                            min: 40101,
                            max: 40200,
                        },
                    },
                ],
            });

            producingTransport.on('icestatechange', (state)=>console.log("$$$$$$$$$$$$ICE producing transport state changed ", state));
            producingTransport.on('dtlsstatechange', (state)=>console.log("$$$$$$$$$$$$DTLS producing transport state changed ", state));

            consumingTransport.on('icestatechange', (state)=>console.log("$$$$$$$$$$$$ICE consuming transport state changed ", state));
            consumingTransport.on('dtlsstatechange', (state)=>console.log("$$$$$$$$$$$$DTLS consuming transport state changed ", state));

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
            socket.to(this.roomId).emit("newProducer", {producerId: producer.id, socketId: socket.id});
            console.log("Propagated Producer Id", producer.id);
            return {id: producer.id}
        }
        else{
            console.log("Matching transport not found");
            return {id: null}
        }
    }


    async consume(socket: Socket<DefaultEventsMap, DefaultEventsMap, DefaultEventsMap, any>, { producerId, rtpCapabilities }: {producerId: string, rtpCapabilities: types.RtpCapabilities}){
        const participant = this.participants.find(elt => elt.socketId === socket.id);
        if (participant && this.router && this.router.canConsume({ producerId, rtpCapabilities })){
            if (participant.consumingTransport){
                let alreadyConsumed = false;
                for (const [key, value] of participant.producers){
                    if (key === producerId){
                        alreadyConsumed = true;
                    }
                }
                if (! alreadyConsumed){
                    const consumer = await participant.consumingTransport.consume({
                        producerId, rtpCapabilities, paused: false,
                    });
                    participant.consumers.set(consumer.id, consumer);
                    return {
                        id: consumer.id,
                        producerId,
                        kind: consumer.kind,
                        rtpParameters: consumer.rtpParameters,
                    };
                }
                console.log("Already consumed this producer");
                return {error: "Already consumed this produce"}
                
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