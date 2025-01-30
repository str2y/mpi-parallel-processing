#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <mpi.h>

#define TAMANHO_MAX_LINHA 1024
#define PALAVRAS_MAX 10000
#define ARTISTAS_MAX 200

typedef struct{
    char palavra[50];
    int count;
}countPalavra;

typedef struct{
    char artista[50];
    int count;
}countArtista;

int compare_artista(const void *x_void, const void *y_void){
	countArtista *x = (countArtista *)x_void;
	countArtista *y = (countArtista *)y_void;
	return (y->count - x->count);
}

int compare_palavra(const void *x_void, const void *y_void){
	countPalavra *x = (countPalavra *)x_void;
	countPalavra *y = (countPalavra *)y_void;
	return (y->count - x->count);
}

void minuscula(char *str){
	for (int i = 0; str[i]; i++){
		str[i] = tolower((unsigned char)str[i]);
	}
}

int main(int argc, char *argv[]){
	MPI_Init(&argc, &argv);
	
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
    
	FILE *file_letras;
	FILE *file_artist;
	char linha[TAMANHO_MAX_LINHA];
	countPalavra count_palavras[PALAVRAS_MAX];
	countArtista count_artista[ARTISTAS_MAX];
	int count_total = 0;
	int count_unica = 0;
	
	
	if (rank != 0){
		file_letras = fopen("letras.csv", "r");

		fseek(file_letras, 0, SEEK_END);
		long filesize = ftell(file_letras);

		long chunk_size = filesize / (size-1);
		long start_offset = (rank-1) * chunk_size;
		fseek(file_letras, start_offset, SEEK_SET);
		
		if (rank != size - 1){
			long bytes_read = 0;
			fgets(linha, sizeof(linha), file_letras);
			while (bytes_read < chunk_size && fgets(linha, sizeof(linha), file_letras)){
				bytes_read += strlen(linha);
				char *token = strtok(linha, " ,");
				while (token != NULL){
					minuscula(token);
					int achado = 0;
					for (int i = 0; i < count_unica; i++){
						if (strcmp(count_palavras[i].palavra, token) == 0){
							count_palavras[i].count++;
							achado = 1;
							break;
						}
					}
					if (!achado && count_unica < PALAVRAS_MAX){
						strcpy(count_palavras[count_unica].palavra, token);
						count_palavras[count_unica].count = 1;
						count_unica++;
					}
					count_total++;
					token = strtok(NULL, " ,");
				}
			}
		} else{
			while (fgets(linha, sizeof(linha), file_letras)){
				char *token = strtok(linha, " ,");
				while (token != NULL) {
					minuscula(token);
					int achado = 0;
					for (int i = 0; i < count_unica; i++){
						if (strcmp(count_palavras[i].palavra, token) == 0){
							count_palavras[i].count++;
							achado = 1;
							break;
						}
					}
					if (!achado && count_unica < PALAVRAS_MAX){
						strcpy(count_palavras[count_unica].palavra, token);
						count_palavras[count_unica].count = 1;
						count_unica++;
					}
					count_total++;
					token = strtok(NULL, " ,.");
				}
			}
		}

		fclose(file_letras);
		MPI_Send(count_palavras, PALAVRAS_MAX * sizeof(countPalavra), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
		MPI_Send(&count_total, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
		MPI_Send(&count_unica, 1, MPI_INT, 0, 2, MPI_COMM_WORLD);
	} else{
		file_artist = fopen("artistas.csv", "r");
		count_total = 0;
		count_unica = 0;
		while (fgets(linha, sizeof(linha), file_artist)){
			char *token = strtok(linha, ",");
			while (token != NULL){
				minuscula(token);
				int achado = 0;
				for (int i = 0; i < count_unica; i++){
					if (strcmp(count_artista[i].artista, token) == 0){
						count_artista[i].count++;
						achado = 1;
						break;
					}
				}
				if (!achado && count_unica < ARTISTAS_MAX){
					strcpy(count_artista[count_unica].artista, token);
					count_artista[count_unica].count = 1;
					count_unica++;
				}
				count_total++;
				token = strtok(NULL, " ,");
			}
		}
		int total_artista = count_total;
		fclose(file_artist);

		for (int i = 1; i < size; i++){
			countPalavra temp[PALAVRAS_MAX];
			int total_count = 0;
			int unique_count = 0;
			MPI_Recv(temp, PALAVRAS_MAX * sizeof(countPalavra), MPI_BYTE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(&total_count, 1, MPI_INT, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			MPI_Recv(&unique_count, 1, MPI_INT, i, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
			for (int j = 0; j < unique_count; j++){
				int achado = 0;
				for (int k = 0; k < count_unica; k++){
					if (strcmp(count_palavras[k].palavra, temp[j].palavra) == 0){
						count_palavras[k].count += temp[j].count;
						achado = 1;
						break;
					}
				}
				if (!achado && count_unica < PALAVRAS_MAX) {
					strcpy(count_palavras[count_unica].palavra, temp[j].palavra);
					count_palavras[count_unica].count = temp[j].count;
					count_unica++;
				}
			}
			count_total += total_count;
		}
		int total_palavra = count_total;
		int numArtist = sizeof(count_artista) / sizeof(count_artista[0]);
		qsort(count_artista,numArtist,sizeof(countArtista),compare_artista);
		printf("Artistas com mais músicas:\n\n");
		for (int i = 0; i < 5; i++){
			printf("%s%d Músicas\n\n", count_artista[i].artista, count_artista[i].count);
		}
		printf("\n%d Músicas totais\n",total_artista);
		
		printf("\n\n\n\nPalavras mais repetidas:\n\n");
		int numPalavra = sizeof(count_palavras) / sizeof(count_palavras[0]);
		qsort(count_palavras,numPalavra,sizeof(countPalavra),compare_palavra);
		for (int i = 0; i < 5; i++){
			printf("%s: %d vezes\n", count_palavras[i].palavra, count_palavras[i].count);
		}
		printf("\n");
		printf("%d Palavras totais\n",total_palavra);
	}

	MPI_Finalize();
	return 0;
}

