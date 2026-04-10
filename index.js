//Worker: index.js
const {SQSClient, ReceiveMessageCommand, DeleteMessageCommand} = require("@aws-sdk/client-sqs");
const {pool} = require('./config/db');
const sqsClient = new SQSClient({region: process.env.AWS_REGION})

async function procesarCola(){
	while(true){
		const {Messages} = await sqsClient.send(new ReceiveMessageCommand({
			QueueUrl: process.env.SQS_QUEUE_URL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds: 5
		}));
		if(Message){
			for(const mensaje of Messages){
				const producto = JSON.parse(mensaje.Body);
				try{
					#insertar en RDS
					await pool.query('INSERT INTO productos (nombre, stock) VALUES ($1, $2)', [producto.nombre, producto.stock]);
					console.log(`producto insertado: ${producto.nombre}`);
					#borrar el mensaje, si no se borra, sqs lo dara a otro worker y se duplicara la peticion
					await pool.query(new DeleteMessageCommand({
						QueueUrl: process.env.SQS_QUEUE_URL,
						ReceiptHandle: mensaje.ReceiptHandle
					}));
				}catch(error){
					console.error("Fallo la insercion, el mensaje volvera a la cola.", error);
				}
			}
		}
	}
}
procesarCola();
