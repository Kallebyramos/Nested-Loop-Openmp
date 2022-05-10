#include <stdlib.h>
#include <stdio.h>
#include <omp.h>
#include <string.h>

typedef struct customer{
	char custkey[10];
	char name[50];
	char address[50];
	char nationkey[50];
	char phone[20];
	char acctbal[50];
	char mktsegment[30];
	char comment[50];
}customer;

typedef struct orders{
	char orderkey[1];
	char custkey[10];
	char orderstatus[1];
	char totalprice[15];
	char orderdate[10];
	char orderpriority[15];
	char clerk[15];
	char shippriority[1];
	char comment[50];
}orders;

/*void printTableCustomer(customer values[]){
	for(int i = 0; i <= 100; i++){
		printf("custkey-> %s, name-> %s, address-> %s, nationkey-> %s, phone-> %s, acctbal-> %s, mktsegment-> %s, comment-> %s\n", values[i].custkey, values[i].name, values[i].address, values[i].nationkey, values[i].phone, 
		values[i].acctbal, values[i].mktsegment, values[i].comment);
	}
}*/

/*void printTableOrders(orders values_o[]){
	for(int i = 0; i <= 200; i++){
		printf("orderkey-> %s, custkey-> %s, orderstatus-> %s, totalprice-> %s, orderdate-> %s, orderpriority-> %s, clerk-> %s, shippriority-> %s, comment-> %s\n", values_o[i].orderkey, values_o[i].custkey, values_o[i].orderstatus, values_o[i].totalprice, values_o[i].orderdate, 
		values_o[i].orderpriority, values_o[i].clerk, values_o[i].shippriority, values_o[i].comment);
	}
}*/


void nested_loop(customer values[], orders values_o[]){
	//#pragma omp target teams distribute parallel for collapse(1) thread_limit(20)
	#pragma omp parallel for num_threads(2)
	for(int i = 0; i <= 100; i++) {
	    	for (int j = 0; j <= 200; j++) {
	        	printf("custkey = %d, orderkey = %d, threadId = %d, NTeams = %d \n", values[i].custkey, values_o[j].orderkey, omp_get_thread_num(), omp_get_num_teams());
	    	}
		}
}


int main(int argc, char*argv[]){
	
    // to store the execution time of code
    double t = 0.0;
    double tb, te;
    
    tb = omp_get_wtime();
	
	//customer file
	FILE* file = fopen("C:\\customer.csv", "r"); //open in read mode
	if(!file){
		printf("Error occured");
		return 0;
	}
	
	char buffer[1024]; //stores the first 1024lines into buffer
	int r_count = 0;
	int f_count = 0;
	
	customer values[102];  //array to store values
	
	int i = 0;
	
	while(fgets(buffer, 1024, file)){
		f_count = 0;
		r_count++;
		if(r_count == 1)
			continue;
		
		char *field = strtok(buffer, "|");
		
		while(field){
			if(f_count == 0)
				strcpy(values[i].custkey, field);
			if(f_count == 1)
				strcpy(values[i].name, field);
			if(f_count == 2)
				strcpy(values[i].address, field);
			if(f_count == 3)
				strcpy(values[i].nationkey, field);
			if(f_count == 4)
				strcpy(values[i].phone, field);
			if(f_count == 5)
				strcpy(values[i].acctbal, field);
			if(f_count == 6)
				strcpy(values[i].mktsegment, field);
			if(f_count == 7)
				strcpy(values[i].comment, field);
			
			field = strtok(NULL, "|"); //update field value
			f_count++;
		}
		i++;
	}
	fclose(file);
	
	//orders file
	FILE* file_orders = fopen("C:\\orders.csv", "r"); //open in read mode
	if(!file_orders){
		printf("Error occured");
		return 0;
	}
	
	char buffer_orders[1024]; //stores the first 1024lines into buffer
	int ro_count = 0;
	int fo_count = 0;
	
	orders values_o[200];  //array to store values
	
	int j = 0;
	
	while(fgets(buffer_orders, 1024, file_orders)){
		ro_count = 0;
		fo_count++;
		if(ro_count == 1)
			continue;
		
		char *field_orders = strtok(buffer_orders, "|");
		
		while(field_orders){
			if(fo_count == 0)
				strcpy(values_o[j].orderkey, field_orders);
			if(fo_count == 1)
				strcpy(values_o[j].custkey, field_orders);
			if(fo_count == 2)
				strcpy(values_o[j].orderstatus, field_orders);
			if(fo_count == 3)
				strcpy(values_o[j].totalprice, field_orders);
			if(fo_count == 4)
				strcpy(values_o[j].orderdate, field_orders);
			if(fo_count == 5)
				strcpy(values_o[j].orderpriority, field_orders);
			if(fo_count == 6)
				strcpy(values_o[j].clerk, field_orders);
			if(fo_count == 7)
				strcpy(values_o[j].shippriority, field_orders);
			if(fo_count == 8)
				strcpy(values_o[j].comment, field_orders);
			
			field_orders = strtok(NULL, "|"); //update field value
			fo_count++;
		}
		j++;
	}
	fclose(file_orders);
	
	//print the results
	
	nested_loop(values, values_o);
	
	te = omp_get_wtime();
	t = te -tb;
	
	printf("\nTime of kernel: %lf\n", t);

		
	return 0;
}