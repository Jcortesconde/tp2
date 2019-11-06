#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <iostream>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mutex>
#include <mpi.h>
#include <map>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
mutex block_mtx;
Block block_send;
Block block_recive;

void printBlock(const Block &block){
	string hash = block.block_hash;
	string previous_hash = block.previous_block_hash;
	cout << endl
	<< " ----- BLOQUE ----- "
	<< endl
	<< "INDEX: " << block.index 
	<< endl
	<< "OWNER: " << block.node_owner_number
	<< endl
	<< "DIFF: " << block.difficulty
	<< endl
	<< "CREATED AT: "<< block.created_at
	<< endl
	<< "NONCE: " << block.nonce
	<< endl
	<< "HASH: " << hash
	<< endl
	<< "PREVIOUS BLOCK HASH: " << previous_hash
	<< endl
	<< " ------------------ "
	<< endl ;
}
//Printea la blockchain desde last_block_in_chain
void printBlockchain(){
	Block debug_chain = *last_block_in_chain;
	int max_iter = 0;
	while(debug_chain.index > 1 && max_iter < VALIDATION_BLOCKS){
		printBlock(debug_chain);
		string debug_chain_previous_block_hash_string = debug_chain.previous_block_hash;
		debug_chain = node_blocks.find(debug_chain_previous_block_hash_string)->second;
		max_iter++;
	}
	printBlock(debug_chain);
}

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){
	//Enviar mensaje TAG_CHAIN_HASH
	MPI_Send(rBlock, 1, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_HASH, MPI_COMM_WORLD);
	
	Block *blockchain = new Block[VALIDATION_BLOCKS];
	MPI_Status status_rcv;
	//Recibir mensaje TAG_CHAIN_RESPONSE
	MPI_Recv(blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, rBlock->node_owner_number, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD, &status_rcv);
	
	//Verificar que los bloques recibidos
	//sean válidos y se puedan acoplar a la cadena
	int blockchain_size;
	//Cuanto la cantidad de bloques recibidos
	MPI_Get_count(&status_rcv, *MPI_BLOCK, &blockchain_size);
	const Block *primer_bloque = blockchain;

	//Si el primer bloque no tiene el hash pedido o el index, descarto la cadena
	string primer_bloque_hash = (*primer_bloque).block_hash;
	string rBlock_hash = (*rBlock).block_hash;
	if(primer_bloque_hash != rBlock_hash || (*primer_bloque).index != (*rBlock).index){		
		delete []primer_bloque;
		return false;
	}
	
	//Reviso uno por uno los hashes de la cadena, asegurandome que coincidan 
	//con el contenido de cada bloque
	for(int i = 0; i < blockchain_size; i++){
		string hash;
		block_to_hash(blockchain, hash);
		//Veo que el hash del bloque sea correcto, si no, descarto la cadena.
		string current_block_hash = blockchain->block_hash;
		if(current_block_hash != hash){
			delete[] primer_bloque;
			return false;
		}
		blockchain++;
	}

	blockchain = (Block*)primer_bloque;

	//Me fijo que el previous_block_hash de cada bloque coincida con el hash del bloque anterior
	//Idem para los indices
	//Notar que itero hasta blockchain_size - 1
	for(int i = 0; i < blockchain_size - 1; i++){
		Block* siguiente = blockchain;
		siguiente++;

		string previous_block_hash_string = blockchain->previous_block_hash;
		string siguiente_block_hash_string =  siguiente->block_hash;

		if(previous_block_hash_string != siguiente_block_hash_string){
			delete[] primer_bloque;
			return false;
		}
		if(siguiente->index != blockchain->index - 1){
			delete[] primer_bloque;
			return false;
		}		
		blockchain++;
	}	
    
	//Si llegue hasta aca, la cadena es valida.
	//Ahora busco el primer bloque en común que tengan las cadenas
	Block* iterador = (Block*)primer_bloque;
	Block* bloque_comun = nullptr;
	for(int i = 0; i < blockchain_size; i++){
		string iterador_hash;
		block_to_hash((const Block*)iterador,iterador_hash);
		auto block_it = node_blocks.find(iterador_hash);
		if(block_it != node_blocks.end()){
			bloque_comun = iterador;
			break;
		}
		iterador++; 
	}
	
	//Si no tengo bloques en comun y el ultimo bloque no tiene como anterior al last_block del receptor elimino la cadena
	string previous_block_hash_string = iterador->previous_block_hash;
	string last_block_in_chain_hash_string = last_block_in_chain->block_hash;

	if(bloque_comun == nullptr /*&& previous_block_hash_string != last_block_in_chain_hash_string*/){
		delete[] primer_bloque;
		return false;
	}
	

	//Borro los bloques que no tengan en común las cadenas
	Block* iterador_dicc = last_block_in_chain;
	string iterador_dicc_hash;
	string bloque_comun_hash;
	block_to_hash((const Block*)bloque_comun,bloque_comun_hash);
	block_to_hash((const Block*)iterador_dicc,iterador_dicc_hash);
	while(iterador_dicc_hash != bloque_comun_hash){
		string anterior_bloque_hash = iterador_dicc->previous_block_hash;
		node_blocks.erase(iterador_dicc_hash);
		iterador_dicc = &node_blocks.find(anterior_bloque_hash)->second; 
		block_to_hash((const Block*)iterador_dicc,iterador_dicc_hash);
	}

	//Agrego los bloques que me faltan para completar el blockchain
	iterador = (Block*)primer_bloque;
	while(!(iterador == bloque_comun)){
		string iterador_hash;
		block_to_hash((const Block*)iterador,iterador_hash);
		node_blocks.insert(std::pair<string, Block>(iterador_hash,*iterador));
		iterador++; 
	}		
	
	//Cambio el ultimo bloque por el ultimo bloque recibido en blockchain
	block_to_hash(primer_bloque,primer_bloque_hash);
	*last_block_in_chain = node_blocks[primer_bloque_hash];
	
	delete []primer_bloque;
	return true;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
	if(valid_new_block(rBlock)){
		string last_block_in_chain_hash = (*last_block_in_chain).block_hash;
		string rBlock_previous_hash = (*rBlock).previous_block_hash;

		//Si el índice del bloque recibido es 1
		//y mí último bloque actual tiene índice 0,
		//entonces lo agrego como nuevo último.
		if(rBlock->index == 1 && last_block_in_chain->index == 0){
			string hash_hex_str;
			Block b = *rBlock;
			hash_hex_str = b.block_hash;
			node_blocks[hash_hex_str] = b;
			*last_block_in_chain = node_blocks[hash_hex_str];
			printf("[%d] Agregado a la lista bloque co index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
			return true;
		}


		//Si el índice del bloque recibido es
		//el siguiente a mí último bloque actual,
		//y el bloque anterior apuntado por el recibido es mí último actual,
		//entonces lo agrego como nuevo último.
		if(rBlock->index == last_block_in_chain->index + 1 && rBlock_previous_hash == last_block_in_chain_hash){			
			string hash_hex_str;
			Block b = *rBlock;
			hash_hex_str = b.block_hash;
			node_blocks[hash_hex_str] = b;
			*last_block_in_chain = node_blocks[hash_hex_str];
			printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
			return true;
		}
		
		

			
		//Si el índice del bloque recibido es
		//el siguiente a mí último bloque actual,
		//pero el bloque anterior apuntado por el recibido no es mí último actual,
		//entonces hay una blockchain más larga que la mía.
		if(rBlock->index == last_block_in_chain->index + 1 && rBlock_previous_hash != last_block_in_chain_hash){
			printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
			bool res = verificar_y_migrar_cadena(rBlock,status);
			return res;
		}

		//Si el índice del bloque recibido es igual al índice de mi último bloque actual,
		//entonces hay dos posibles forks de la blockchain pero mantengo la mía
		if(rBlock->index == last_block_in_chain->index){
			printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
			return false;
		}
	

		//Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
		//entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
		if(rBlock->index < last_block_in_chain->index){
			printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n",mpi_rank,rBlock->index,last_block_in_chain->index, status->MPI_SOURCE);
			return false;
		}
			

		//Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
		//entonces me conviene abandonar mi blockchain actual
		if(rBlock->index > last_block_in_chain->index + 1){
			printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
			bool res = verificar_y_migrar_cadena(rBlock,status);
			return res;
		}

			

	}

	printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
	return false;
}


//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block){
	//No enviar a mí mismo y enviar en distinto orden
	for(int i = 1; i < total_nodes; i++){		
		block_send = *(Block*)block;
		MPI_Send(block, 1, *MPI_BLOCK, (mpi_rank + i) % total_nodes, TAG_NEW_BLOCK, MPI_COMM_WORLD);		
	}
	
}

//Proof of work
void* proof_of_work(void *ptr){
		string hash_hex_str;
		Block block;
		unsigned int mined_blocks = 0;
		while(true){
			block_mtx.lock();
			block = *last_block_in_chain;
			block_mtx.unlock();
			//Preparar nuevo bloque
			block.index += 1;
			block.node_owner_number = mpi_rank;
			block.difficulty = DEFAULT_DIFFICULTY;
			block.created_at = static_cast<unsigned long int> (time(NULL));
			memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);
		
			//Agregar un nonce al azar al bloque para intentar resolver el problema
			gen_random_nonce(block.nonce);

			//Hashear el contenido (con el nuevo nonce)
			block_to_hash(&block,hash_hex_str);

			//Contar la cantidad de ceros iniciales (con el nuevo nonce)
			if(solves_problem(hash_hex_str)){
					//Verifico que no haya cambiado mientras calculaba		
					block_mtx.lock();			
					if(last_block_in_chain->index < block.index){
						mined_blocks += 1;
						*last_block_in_chain = block;
						strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
						node_blocks[hash_hex_str] = *last_block_in_chain;		
						if(node_blocks.size() == MAX_BLOCKS){
							block_mtx.unlock();
							//Si un minero llega a MAX_BLOCKS termino la ejecución del sistema
							MPI_Abort(MPI_COMM_WORLD, 1);
							return NULL;
						}
						printf("[%d] Agregué un producido con index %d \n",mpi_rank, last_block_in_chain->index);
						
						//Mientras comunico, no responder mensajes de nuevos nodos
						broadcast_block(last_block_in_chain);
					}
					block_mtx.unlock();
					

			}

		}

		return NULL;
}


int node(){

	//Tomar valor de mpi_rank y de nodos totales
	MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

	//La semilla de las funciones aleatorias depende del mpi_ranking
	srand(time(NULL) + mpi_rank);
	printf("[MPI] Lanzando proceso %u\n", mpi_rank);

	last_block_in_chain = new Block; 
	//Inicializo el primer bloque
	last_block_in_chain->index = 0;
	last_block_in_chain->node_owner_number = mpi_rank;
	last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
	last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
	memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

	//Crear thread para minar
	pthread_t miner;
	pthread_create(&miner, nullptr, proof_of_work, nullptr);
		
	
	while(true){
			//Recibir mensajes de otros nodos
			MPI_Status status; // Me guardo el status del mensaje recibido para poder acceder a su tag
			MPI_Recv(&block_recive, 1, *MPI_BLOCK, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			
			//Si es un mensaje de nuevo bloque, llamar a la función
			if(status.MPI_TAG == TAG_NEW_BLOCK){
				//Valido el nuevo bloque recibido
				block_mtx.lock();
				validate_block_for_chain(&block_recive, &status);
				block_mtx.unlock();
			}

			//Si es un mensaje de pedido de cadena,
   			//responderlo enviando los bloques correspondientes
			else if(status.MPI_TAG == TAG_CHAIN_HASH){
				Block *blockchain = new Block[VALIDATION_BLOCKS];
				const Block* primer_bloque = blockchain;
				*blockchain = block_recive;	
				int count = 1;
				for(int i = 1; i < VALIDATION_BLOCKS && blockchain->index > 1; i++){
					string hash_string = block_recive.previous_block_hash;
			 		block_recive = node_blocks.find(hash_string)->second;
			 		blockchain++;
					*blockchain = block_recive;
					count++;
				}
				block_mtx.lock();
				MPI_Send(primer_bloque, count, *MPI_BLOCK, status.MPI_SOURCE, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD);
				block_mtx.unlock();
				delete[] primer_bloque;
			}

	}
	
	delete last_block_in_chain;
	return 0;
}