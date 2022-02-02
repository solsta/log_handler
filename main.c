#include <stdio.h>
#include <string.h>
#include <libpmemlog.h>
#include <dirent.h>
#include <stdlib.h>
#include <libpmemobj/base.h>
#include <libpmemobj/pool_base.h>
#include <libpmemobj/types.h>
#include <unistd.h>
#include <library.h>
#include <libpmemobj/tx_base.h>
#include <stdbool.h>

#define POOL_SIZE 8000000
#define MAX_OPS_IN_THE_LOG 1
#define OPERATIONS_TO_PRODUCE 10000
#define MAX_INDEX_VALUE 1000
#define SM_PMEM_DIRECTORY "/mnt/dax/test_outputs/pmem_logs/sm/"

PMEMlogpool *plp_src;
static PMEMobjpool *pop;
static PMEMoid root;
struct my_root *rootp;
struct state_machine sm;

struct my_root {
    int initialised;
    int processed_entries;
    int ops_to_skip;
    char file_name[200];
    char file_status[20];
};

struct state_machine {
    int *persistent_state;
    int *intermediate_state;
};

/* State machine method definition */
void state_machine_initialise();
void state_machine_execute_command(char command);
/* end */

bool file_exists(const char *path){
    return access(path, F_OK) != 0;
}

static void
log_stages(PMEMobjpool *pop_local, enum pobj_tx_stage stage, void *arg)
{
    /* Commenting this out because this is not required during normal execution. */
    /* dr_fprintf(STDERR, "cb stage: ", desc[stage], " "); */
}

void log_commit_flag(char *command){
    //printf("Logging commit flag\n");
    if(pop == NULL){
        //printf("META file has not been initialised!\n");
        exit(1);
    }
    pmemobj_tx_begin(pop, NULL, TX_PARAM_CB, log_stages, NULL,
                     TX_PARAM_NONE);
    pmemobj_tx_add_range_direct(&rootp->processed_entries, sizeof(int));
    rootp->processed_entries = rootp->processed_entries +1;
    pmemobj_tx_commit();
    pmemobj_tx_end();
    //printf("Logged\n");
}

int to_skip;
static int
process_log(const void *buf, size_t len, void *arg)
{   /* Logic to iterate over each command individually */
    //int skip_records = (int) arg;
    //printf("will skip %d operations in this log\n", to_skip);
    char tmp;
    int num_of_elements = 0;
    memcpy(&tmp, buf, 1);
    /* Allowed values */
    while(tmp == 'i' || tmp == 'd'){
        /* TODO Insert state machine code invoke here */
            state_machine_execute_command(tmp);
            num_of_elements = num_of_elements + 1;
            memcpy(&tmp, buf + num_of_elements, 1);
            sleep(1);

            log_commit_flag("1");
    }
    /* File processing is finished, can open next sm logfile and new psm log file */
    return 0;
}

static int
process_log_with_skip(const void *buf, size_t len, void *arg)
{   /* Logic to iterate over each command individually */
    //int skip_records = (int) arg;
    //printf("will skip %d operations in this log\n", to_skip);
    char tmp;
    int num_of_elements = 0;
    memcpy(&tmp, buf, 1);
    /* Allowed values */
    while(tmp == 'i' || tmp == 'd'){

        if(num_of_elements <= rootp->ops_to_skip){
            //printf("Skipping\n");
            num_of_elements = num_of_elements + 1;
        } else{
            //printf("Processing\n");
            /* TODO Insert state machine code invoke here */
            state_machine_execute_command(tmp);
            num_of_elements = num_of_elements + 1;
            memcpy(&tmp, buf + num_of_elements, 1);
            //sleep(1);
            log_commit_flag("1");
        }


    }
    /* File processing is finished, can open next sm logfile and new psm log file */
    return 0;
}

char *get_next_available_file_name(char *directory_path){
    /* https://stackoverflow.com/questions/4204666/how-to-list-files-in-a-directory-in-a-c-program */
    DIR *directory;
    struct dirent *dir;
    directory = opendir(directory_path);
    int max_index = 0;
    if (directory) {
        while ((dir = readdir(directory)) != NULL) {
            int current_index = atoi(dir->d_name);
            if(max_index < current_index){
                max_index = current_index;
            }
            //printf("%s\n", dir->d_name);
        }
        closedir(directory);
    }

    max_index = max_index +1;

    char *sequence_number = malloc(3);
    char *new_file_ptr = calloc(strlen(directory_path) + strlen(sequence_number), 1);
    sprintf(sequence_number, "%d",max_index);
    strcat(new_file_ptr, directory_path);
    strcat(new_file_ptr, sequence_number);

    return new_file_ptr;
}

PMEMlogpool *create_log_file() {
    char *path;
    /* Change to a unique and increasing name */
    path = get_next_available_file_name(SM_PMEM_DIRECTORY);
    PMEMlogpool *plp;
    /* create the pmemlog pool or open it if it already exists */
    plp = pmemlog_create(path, POOL_SIZE, 0666);
    printf("Ran create log command\n");
    if (plp == NULL) {
        printf("Fails to create pmemlog\n");
        plp = pmemlog_open(path);
    }
    if (plp == NULL) {
        perror(path);
        printf("Fails to open pmemlog\n");
    }
    return plp;
}

int op_count;
void log_command(char *command){

    if(op_count < MAX_OPS_IN_THE_LOG){
        printf("About to append!\n");
        if (pmemlog_append(plp_src, command, strlen(command)) < 0) {
            perror("pmemlog_append");
            exit(1);
        }
        printf("Appended!\n");
        op_count = op_count+1;
        /* Adding this for simplicity of testing */
        if(op_count == MAX_OPS_IN_THE_LOG){
            pmemlog_close(plp_src);
            plp_src = create_log_file();
            op_count = 0;
        }

    } else{
        /* Create a new log file with a new name and reassign PLP 8 */
        pmemlog_close(plp_src);
        plp_src = create_log_file();
        op_count = 0;
        if (pmemlog_append(plp_src, command, strlen(command)) < 0) {
            perror("pmemlog_append");
            exit(1);
        }
        op_count = op_count+1;
    }


}

void run_producer(){
    plp_src = create_log_file();
    for(int i = 0; i< OPERATIONS_TO_PRODUCE; i++){
        /* Use this to log a command */
        log_command("d");
        log_command ("i");
    }
    pmemlog_close(plp_src);
}

char *get_lowest_index_that_can_be_processed(char *directory_path){
    DIR *directory;
    struct dirent *dir;
    directory = opendir(directory_path);
    int min_index = MAX_INDEX_VALUE;
    int files_in_the_directory = 0;
    if (directory) {
        while ((dir = readdir(directory)) != NULL) {
            if(strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0) {
                int current_index = atoi(dir->d_name);
                files_in_the_directory = files_in_the_directory + 1;
                if (min_index > current_index) {
                    min_index = current_index;
                }
                //printf("File: %s\n", dir->d_name);
            }
        }
        closedir(directory);
    }
    //printf("Lowest available index is %d\n", min_index);
    char *sequence_number = malloc(3);
    sprintf(sequence_number, "%d",min_index);

    if(files_in_the_directory <= 1){
        //printf("Nothing to consume, waiting\n");
        char *last_file = calloc(strlen(directory_path)+strlen(sequence_number), 1);
        strcat(last_file, directory_path);
        strcat(last_file, sequence_number);
        /* Try opening a file and if this fails - wait */
        plp_src = pmemlog_open(last_file);

        if (plp_src == NULL) {
            //printf("Last log file is currently being used\n");
            //printf("Last file: %s\n", last_file);
            //perror("File open\n");
        } else{
            //printf("Consuming the last log file\n");
            pmemlog_close(plp_src);
            return sequence_number;
        }
        //printf("No more files!\n");
        //exit(0);
        return NULL;
    }
    return sequence_number;
}

char *concat_dir_and_filename(char *dir_name, char *file_name){
    char *full_path = calloc(strlen(dir_name)+ strlen(file_name), 1);
    strcat(full_path, dir_name);
    strcat(full_path, file_name);
    return full_path;
}

void run_consumer(){
    char *index;
    index = get_lowest_index_that_can_be_processed(SM_PMEM_DIRECTORY);
    if (index == NULL){
        sleep(1);
        printf("Waiting for log to process.\n");
        run_consumer();
    }
    /* Process */
    char *plp_src_path = concat_dir_and_filename(SM_PMEM_DIRECTORY, index);
    plp_src = pmemlog_open(plp_src_path);

    if (plp_src == NULL) {
        printf("Can't open this file\n");
        perror(plp_src_path);
        exit(1);
    }
    /* Set the currently processed file */
    pmemobj_tx_begin(pop, NULL, TX_PARAM_CB, log_stages, NULL,
                     TX_PARAM_NONE);
    pmemobj_tx_add_range_direct(rootp->file_name, strlen(rootp->file_name));
    pmemobj_tx_add_range_direct(rootp->file_status, strlen(rootp->file_status));
    memcpy(rootp->file_name, plp_src_path, strlen(plp_src_path));
    memcpy(rootp->file_status, "IN PROGR", strlen("IN PROGR"));

    pmemobj_tx_commit();
    pmemobj_tx_end();

    pmemlog_walk(plp_src, 0, process_log, NULL);

    pmemobj_tx_begin(pop, NULL, TX_PARAM_CB, log_stages, NULL,
                     TX_PARAM_NONE);
    pmemobj_tx_add_range_direct(&rootp->processed_entries, sizeof(int));
    pmemobj_tx_add_range_direct(rootp->file_status, strlen(rootp->file_status));
    memcpy(rootp->file_status, "COMPLETE", strlen("COMPLETE"));
    rootp->processed_entries = 0;
    pmemobj_tx_commit();
    pmemobj_tx_end();

    /* Delete */
    pmemlog_close(plp_src);


    if (remove(plp_src_path) == 0){
        printf("Processing of %s is complete.\n", plp_src_path);
    } else{
        //printf("Could not delete\n");
    }
    run_consumer();
}

void create_unique_file_name(char *path_to_pmem){
    strcat(path_to_pmem, "/mnt/dax/test_outputs/pmem_log");
}

void mmap_meta_file(){
    //printf("Initializing\n");
    /* Change to a safe imlementation */
    char *path_to_pmem = calloc(200, 1);
    create_unique_file_name(path_to_pmem);

    if (file_exists((path_to_pmem)) != 0) {
        if ((pop = pmemobj_create(path_to_pmem, POBJ_LAYOUT_NAME(list),
                                  PMEMOBJ_MIN_POOL, 0666)) == NULL) {
            perror("failed to create pool\n");
        }
    } else {
        if ((pop = pmemobj_open(path_to_pmem, POBJ_LAYOUT_NAME(list))) == NULL) {
            perror("failed to open pool\n");
        }
    }
    root = pmemobj_root(pop, sizeof(struct my_root));
    rootp = pmemobj_direct(root);
    /**
    if(rootp->number_of_entries == NULL){
        printf("Log is opened for the first time!\n");
        rootp->number_of_entries = 0;
    } else
     **/
    if(rootp->initialised != 1){
        //printf("Initializing the meta file.\n");
        pmemobj_tx_begin(pop, NULL, TX_PARAM_CB, log_stages, NULL,
                         TX_PARAM_NONE);
        pmemobj_tx_add_range_direct(&rootp->initialised, sizeof(int));
        pmemobj_tx_add_range_direct(&rootp->processed_entries, sizeof(int));
        pmemobj_tx_add_range_direct(rootp->file_name, strlen(rootp->file_name));

        rootp->initialised = 1;
        rootp->processed_entries = 0;
        memcpy(rootp->file_name, "NULL", strlen("NULL"));

        pmemobj_tx_commit();
        pmemobj_tx_end();

    } else{
        //printf("Meta file content:");
        //printf("Currently processed file: %s, processed entries: %d\n", rootp->filename, rootp->processed_entries);
    }
}

void print_info(){
    //printf("Initializing\n");
    /* Change to a safe imlementation */
    char *path_to_pmem = calloc(200, 1);
    create_unique_file_name(path_to_pmem);

    if(pop == NULL){
        if (file_exists((path_to_pmem)) != 0) {
            if ((pop = pmemobj_create(path_to_pmem, POBJ_LAYOUT_NAME(list),
                                      PMEMOBJ_MIN_POOL, 0666)) == NULL) {
                perror("failed to create pool\n");
            }
        } else {
            if ((pop = pmemobj_open(path_to_pmem, POBJ_LAYOUT_NAME(list))) == NULL) {
                perror("failed to open pool\n");
            }
        }
    }


    root = pmemobj_root(pop, sizeof(struct my_root));
    rootp = pmemobj_direct(root);
    printf("Current file: %s, status - %s, entry: %d\n", rootp->file_name, rootp->file_status, rootp->processed_entries);


}

void execute_recovery(){
    char *path_to_pmem = calloc(200, 1);
    create_unique_file_name(path_to_pmem);

    if(pop == NULL){
        if (file_exists((path_to_pmem)) != 0) {
            if ((pop = pmemobj_create(path_to_pmem, POBJ_LAYOUT_NAME(list),
                                      PMEMOBJ_MIN_POOL, 0666)) == NULL) {
                perror("failed to create pool\n");
            }
        } else {
            if ((pop = pmemobj_open(path_to_pmem, POBJ_LAYOUT_NAME(list))) == NULL) {
                perror("failed to open pool\n");
            }
        }
    }
    root = pmemobj_root(pop, sizeof(struct my_root));
    rootp = pmemobj_direct(root);

    /* Case 1 */
    if (strcmp(rootp->file_status, "COMPLETE") == 0){
        printf("Execution has finished correctly, no need for recovery.\n");
    } else if(strcmp(rootp->file_status, "IN PROGR") == 0){
        /* TODO */
        //Get the status of the last transaction from external file */
        rootp->ops_to_skip = rootp->processed_entries;
        printf("Is about to proceed with %s from operation %d\n", rootp->file_name, rootp->processed_entries);
        char *file_name = rootp->file_name;
        plp_src = pmemlog_open(file_name);
        pmemlog_walk(plp_src, 0, process_log_with_skip, NULL);
        pmemobj_tx_begin(pop, NULL, TX_PARAM_CB, log_stages, NULL,
                         TX_PARAM_NONE);
        pmemobj_tx_add_range_direct(&rootp->processed_entries, sizeof(int));
        pmemobj_tx_add_range_direct(rootp->file_status, strlen(rootp->file_status));
        memcpy(rootp->file_status, "COMPLETE", strlen("COMPLETE"));
        rootp->processed_entries = 0;
        pmemobj_tx_commit();
        pmemobj_tx_end();
        printf("Recovery is complete!\n");
    } else{
        printf("Uknown status: %s\n", rootp->file_status);
    }
}

/* State Machine code */
void run_graceful_exit(){
    /* The only reason for this now is because the signal handlers do not seem to work */
    pmemobj_close(pop);
    exit(0);
}

void state_machine_loop();

static void consume_state_machine_command(char command){

}

void state_machine_execute_command(char command){
    if(command == 'i'){
        for(int i=0; i<5; i++){
            printf("Inside loop\n");
            printf("State: %d, Intermediate state: %d\n", *sm.persistent_state, *sm.intermediate_state);
            *sm.intermediate_state = *sm.intermediate_state + 1;
            sleep(1);
        }
        *sm.persistent_state = *sm.persistent_state+1;
        *sm.intermediate_state = 0;
        printf("Updated state: %d\n", *sm.persistent_state);
    } else if (command == 'd'){
            for(int i=0; i<5; i++){
                printf("State: %d, Intermediate state: %d\n", *sm.persistent_state, *sm.intermediate_state);
                *sm.intermediate_state = *sm.intermediate_state + 1;
                sleep(1);
            }
            *sm.persistent_state = *sm.persistent_state-1;
            *sm.intermediate_state = 0;
            printf("Updated state: %d\n", *sm.persistent_state);
    }

}

void state_machine_loop(){
    /* Wait for the command here */
    printf("Type in action:\n");
    char command[10];
    fgets(command,10,stdin);
    command[strcspn(command, "\r\n")] = 0;
    printf("Starting state: %d\n", *sm.persistent_state);

    if(strcmp(command, "inc") == 0){
        printf("About to log\n");
        log_command("i");
        printf("Logged\n");
        state_machine_execute_command('i');
        printf("Executed state machine op\n");
        state_machine_loop();
    }
    if(strcmp(command, "dec") == 0){
        log_command("d");
        state_machine_execute_command('d');
        state_machine_loop();
    }
    if(strcmp(command, "pri") == 0){
        printf("Current state is : %d\n", *sm.persistent_state);
        state_machine_loop();
    }
    if(strcmp(command, "recpsm") == 0){
        printf("Recovering\n");
        /* This recovery is only for the psm part */
        execute_recovery();
        state_machine_loop();
    }
    if(strcmp(command, "sm") == 0){
        printf("Recovering\n");
        /* TODO  memcpy from pmem pool */

        state_machine_loop();
    }
    if(strcmp(command, "exit") == 0){
        run_graceful_exit();
    }
    printf("You've entered an invalid command!\n");
    state_machine_loop();
}

void start_state_machine(){
    /* Move this within a transaction */
    state_machine_loop();
}
/* State Machine code end */


int main(int argc, char *argv[]) {
    sm.persistent_state = malloc(sizeof (int));
    sm.intermediate_state = malloc(sizeof (int));

    *sm.persistent_state = 0;
    *sm.intermediate_state = 0;
    /* TODO Take a criu snapshot here? */
    if(argc>1){
        if(strcmp(argv[1], "producer") == 0){
            /* Run state machine code from here */
            plp_src = create_log_file();

            start_state_machine();
            //state_machine_loop(persistent_state, intermediate_state);
            //run_producer();
        } else if (strcmp(argv[1], "consumer") == 0){
            mmap_meta_file();
            run_consumer();
        } else if (strcmp(argv[1], "print") == 0) {
            printf("Printing\n");
            print_info();
        } else if(strcmp(argv[1], "recover") == 0){
            printf("Recovering\n");
            execute_recovery();
            //execute_in_recovery_mode();
            /* Find the latest psm file,
             * count it's elements.
             * Open the pmem obj file, which contains head
             * read the status of the last operation.
             * If the operation was commited - increase the number of
             * commited messages by 1.
             * Open sm log and run walk method, while ignoring
             * the number equal to a number of commit markers.
             * */
        }
    }
    return 0;
}
