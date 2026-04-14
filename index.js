//Worker: index.js
require('dotenv').config();
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
		if(Messages){
			for(const mensaje of Messages){
				const producto = JSON.parse(mensaje.Body);
				
				if(!producto.sku){
					const prefijo=producto.nombre.substring(0,3).toUpperCase();
					const sufijo=Math.floor(Math.random()*10000);
					producto.sku=`${prefijo}-${sufijo}`;
				}
				try{
					//insertar en RDS
					await pool.query('INSERT INTO productos (sku, nombre, precio, stock, descripcion) VALUES ($1, $2, $3, $4, $5)', [producto.nombre, producto.precio, producto.stock, producto.descripcion]);
					console.log(`producto insertado: ${producto.nombre}`);
					//borrar el mensaje, si no se borra, sqs lo dara a otro worker y se duplicara la peticion
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
