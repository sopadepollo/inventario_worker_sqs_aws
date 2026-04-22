//Worker: index.js
require('dotenv').config();
const {SQSClient, ReceiveMessageCommand, DeleteMessageCommand} = require("@aws-sdk/client-sqs");
const {writePool} = require('./config/db');
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
				const cuerpoMensaje = JSON.parse(mensaje.Body);
				const {accion, payload} = cuerpoMensaje;
				switch(accion){
					case 'CREAR_PRODUCTO':
						await crearProducto(payload);
						console.log(`*Producto creado: ${payload.nombre}`);
						break;
					case 'ACTUALIZAR_PRODUCTO':
						const {query, valores} = updateProducto(payload.sku, payload.datos);
						await writePool.query(query, valores);
						console.log('*Producto actualizado: ${payload.sku}');
					case 'MOVIMIENTO_INVENTARIO':
						await procesarTransaccionInventario(payload);
						console.log(`*Inventario actualizado: ${payload.sku}`);
						break;
					default:
						console.warn(`(?)Accion desconocida ignorada: ${accion}`);
				}
				await sqsClient.send(new DeleteMessageCommand({
					QueueUrl: process.env.SQS_QUEUE_URL,
					ReceiptHandle: mensaje.ReceiptHandle
				}));
			}
		}
	}
}
procesarCola();

const crearProducto = async (payload) => {
	if(!payload.sku){
		const prefijo=payload.nombre.substring(0,3).toUpperCase();
		const sufijo=Math.floor(Math.random()*10000);
		payload.sku=`${prefijo}-${sufijo}`;
	}
		//insertar en RDS
		await writePool.query('INSERT INTO productos (sku, nombre, precio, stock, descripcion) VALUES ($1, $2, $3, $4, $5)', [payload.sku, payload.nombre, payload.precio, payload.stock, payload.descripcion]);
		console.log(`producto insertado: ${payload.nombre}`);
		//borrar el mensaje, si no se borra, sqs lo dara a otro worker >
		await sqsClient.send(new DeleteMessageCommand({
			QueueUrl: process.env.SQS_QUEUE_URL,
			ReceiptHandle: mensaje.ReceiptHandle
		}));

};

const updateProducto = (sku, payload) => {
	const llaves = Object.keys(payload);
	if(llaves.length === 0) throw new Error("No hay datos que actualizar");
	//construir los valores dados en un formato para insertarlo
	const setQuery = llaves.map((llave, index) => `${llave} = ${index + 1}`).join(', ');
	//se inserta la variable dada a la insercion de datos a dar al db
	const query = `UPDATE productos SET ${setQuery}, actualizado_en = CURRENT_TIMESTAMP WHERE sku = $${llaves.length + 1} RETURNING *`;
	const valores = [...Object.values(payload), sku];
	return {query, valores};
};

const procesarTransaccionInventario = async (payload) => {
	const {sku,cantidad,tipo_movimiento,motivo,usuario_id} = payload;
	const client = await writePool.connect();

	try{
		await client.query('BEGIN');
		//verificar stock actual
		const resProducto = await client.query('SELECT id, stock FROM productos WHERE sku = $1',[sku]);
		if(resProducto.rows.length === 0) throw new Error('Producto no encontrado');
		
		const producto = resProducto.rows[0];
		let nuevoStock = producto.stock;

		//modificar el stock al nuevo stock propuesto
		if(tipo_movimiento === 'ENTRADA') nuevoStock += cantidad;
		else if(tipo_movimiento === 'SALIDA') nuevoStock -= cantidad;
		else throw new Error('Movimiento invalido');

		if(nuevoStock < 0) throw new Error('Stock insuficiente');
		//actualizar y guardar el historial
		await client.query('UPDATE productos SET stock = $1 actualizado_en = CURRENT_TIMESTAMP WHERE id = $2',[nuevoStock, producto.id]);
		
		await client.query(
			`INSERT INTO historial_inventario (producto_id, usuario_id, tipo_movimiento, cantidad, stock_resultante, motivo)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			[producto.id,usuario_id,tipo_movimiento,cantidad,nuevoStock,motivo]
		);
		await client.query('COMMIT');
	}catch(error){
		await client.query('ROLLBACK');
		throw error;
	}finally{
		client.release();
	}
};
