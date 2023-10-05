require('dotenv').config();
const mysql = require('mysql2/promise');
const pgp = require('pg-promise')({
    promiseLib: require('bluebird')
});

const sourceConfig = {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
}
const destinationConfig = {
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD
}


async function migrate() {
try{
    const sourceDb = await mysql.createConnection(sourceConfig);
    const destDb = pgp(destinationConfig);

	await Truncate(destDb,"smartirb.state")
				

		console.log("Migrating Table state...")
		let success = 0 
		let errors = 0 
		try {
			const [rows] = await sourceDb.execute('SELECT * FROM state');
			for (let row of rows) {
				// Do any field Modifications or Add additional fields 

						 	
				row.id = row.id==null?0:row.id 		 	
				row.name = row.name==null?'':row.name 		 	
				row.abbreviation = row.abbreviation==null?'':row.abbreviation 

				await destDb.none('INSERT INTO smartirb.state (id, name, abbreviation) VALUES ($1, $2, $3)', [row.id, row.name, row.abbreviation]).then(
					(res)=>{
						success++
					}
				)
				.catch(
					(err)=>{
						console.log("INSERT ERROR::", err)
						errors++
					}
				);
			}
			console.log('Migration of state table completed!');
			console.log('Success Count::', success);
			console.log('Error Count::', errors);

		} catch (error) {
			console.error('Migration of state failed:', error);
		} finally {
			await sourceDb.end();
			await destDb.$pool.end();
		}

    console.log('Migration completed!');
} catch (error) {
    console.error('Migration failed:', error);
} 
}

function SetMapping(pgConn, Maptable, Field, oldVal, newVal){
    //Insert OldVal and NewValue in to Maptable 
	
}

function GetMapping(pgConn, Maptable, Field, oldVal){
    //Read NewVal for the Old Value from Maptable 
}

async function  RunQuery(dbConn,query){
	const [rows] = await dbConn.query(query);
	return rows
}

async function  Truncate(dbConn,table){
	await dbConn.none("truncate table " + table +";");
}

migrate();