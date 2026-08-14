import {createWorker, types} from "mediasoup";
import "dotenv/config";





// This function creates a list of workers that will be sitting on cpu waiting to be used for calls
const createWorkers = async() => {
    const workers =  [];
    for (let i = 0; i < parseInt(process.env.NUMBER_OF_WORKERS||"1"); i++) {
        const worker = await createWorker({
            logLevel: "debug",
        });
        worker.on('died', () => {
            console.error(`mediasoup Worker ${i} died — must restart process`);
            process.exit(1);
        });
        workers.push(worker);
    }
    return workers;
}

const workers = createWorkers();
export default workers;


// determines if client has right to connect to the call room
export const hasPermission = () => {
    return true
}


const createChatRoom = async(worker: types.Worker) => {

}


export const getChatRoom = () => {

}