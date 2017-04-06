#include <link-cache.h>
#include <active-page-table.h>
#include <nv_memory.h>
#include <nv_utils.h>
#include <epoch.h>

#define CACHE_LINES_PER_NV_NODE 1 

typedef volatile void* VPVOID; 

// callback from Epoch allocator
void finalize_node(void * node, void * context, void* tls) {
  EpochFreeNode(node);
}

seekRecord_t * insseek(thread_data_t * data, long key, int op){
    
    node_t * gpar = NULL; // last node (ancestor of parent on access path) whose child pointer field is unmarked
    node_t * par = data->rootOfTree;
    node_t * leaf;
    node_t * leafchild;
    
    
    AO_t parentPointerWord = 0; // contents in gpar
    AO_t leafPointerWord = par->child.AO_val1; // contents in par. Tree has two imaginary keys \inf_{1} and \inf_{2} which are larger than all other keys. 
    AO_t leafchildPointerWord; // contents in leaf
    
    bool isparLC = false; // is par the left child of gpar
    bool isleafLC = true; // is leaf the left child of par
    bool isleafchildLC; // is leafchild the left child of leaf
    
    
    leaf = (node_t *)get_addr_for_reading(leafPointerWord);
        if(key < leaf->key){
            leafchildPointerWord = leaf->child.AO_val1;
            isleafchildLC = true;
            
        }
        else{
            leafchildPointerWord = leaf->child.AO_val2;
            isleafchildLC = false;
        }
    
    leafchild = (node_t *)get_addr_for_comparing(leafchildPointerWord);
    
    
    
    while(leafchild != NULL){
        if(!is_marked(leafPointerWord)){
            gpar = par;
            parentPointerWord = leafPointerWord;
            isparLC = isleafLC;
        }
        
        par = leaf;
        leafPointerWord = leafchildPointerWord;
        isleafLC = isleafchildLC;
        
        leaf = (node_t *)get_addr_for_reading((AO_t)leafchild);
        
        
        if(key < leaf->key){
            leafchildPointerWord = leaf->child.AO_val1;
            isleafchildLC = true;
        }
        else{
            leafchildPointerWord = leaf->child.AO_val2;
            isleafchildLC = false;
        }    
                
        leafchild = (node_t *)get_addr_for_comparing(leafchildPointerWord);
        
    }
    
    if(key == leaf->key){
    // key matches that being inserted    
      return NULL;
    }
    
    seekRecord_t * R = data->sr;
    
    R->leafKey = leaf->key;
        
    R->parent = par;
    
    R->pL = leafPointerWord;
    
    R->isLeftL = isleafLC;
    
    
    R->lum = gpar;
    R->lumC = parentPointerWord;    
    R->isLeftUM = isparLC;
    return R;
}


seekRecord_t * delseek(thread_data_t * data, long key, int op){
    node_t * gpar = NULL; // last node (ancestor of parent on access path) whose child pointer field is unmarked
    node_t * par = data->rootOfTree;
    node_t * leaf;
    node_t * leafchild;
    
    
    AO_t parentPointerWord = 0; // contents in gpar
    AO_t leafPointerWord = par->child.AO_val1; // contents in par. Tree has two imaginary keys \inf_{1} and \inf_{2} which are larger than all other keys. 
    AO_t leafchildPointerWord; // contents in leaf
    
    bool isparLC = false; // is par the left child of gpar
    bool isleafLC = true; // is leaf the left child of par
    bool isleafchildLC; // is leafchild the left child of leaf
    
    
    leaf = (node_t *)get_addr_for_reading(leafPointerWord);
        if(key < leaf->key){
            leafchildPointerWord = leaf->child.AO_val1;
            isleafchildLC = true;
            
        }
        else{
            leafchildPointerWord = leaf->child.AO_val2;
            isleafchildLC = false;
        }
    
    leafchild = (node_t *)get_addr_for_comparing(leafchildPointerWord);
    
    
    
    while(leafchild != NULL){
        if(!is_marked(leafPointerWord)){
            gpar = par;
            parentPointerWord = leafPointerWord;
            isparLC = isleafLC;
        }
        
        par = leaf;
        leafPointerWord = leafchildPointerWord;
        isleafLC = isleafchildLC;
        
        leaf = (node_t*)get_addr_for_reading((AO_t)leafchild);
        
        
        if(key < leaf->key){
            leafchildPointerWord = leaf->child.AO_val1;
            isleafchildLC = true;
        }
        else{
            leafchildPointerWord = leaf->child.AO_val2;
            isleafchildLC = false;
        }    
        
        leafchild = (node_t *)get_addr_for_comparing(leafchildPointerWord);
        
    }
        
            // op = DEL
    if(key != leaf->key){
      // key is not found in the tree.
        return NULL;
    }
        
    seekRecord_t * R = data->sr;
    
    R->leafKey = leaf->key;
        
    R->parent = par;
    
    R->pL = leafPointerWord;
    
    R->isLeftL = isleafLC;
    
    
    R->lum = gpar;
    R->lumC = parentPointerWord;    
    R->isLeftUM = isparLC;

    return R;
}


seekRecord_t * secondary_seek(thread_data_t * data, long key, seekRecord_t * sr){
    
    node_t * flaggedLeaf = (node_t *)get_addr_for_reading(sr->pL);
    node_t * gpar = NULL; // last node (ancestor of parent on access path) whose child pointer field is unmarked
    node_t * par = data->rootOfTree;
    node_t * leaf;
    node_t * leafchild;
    
    AO_t parentPointerWord = 0; // contents in gpar
    AO_t leafPointerWord = par->child.AO_val1; // contents in par. Tree has two imaginary keys \inf_{1} and \inf_{2} which are larger than all other keys. 
    AO_t leafchildPointerWord; // contents in leaf
    
    bool isparLC = false; // is par the left child of gpar
    bool isleafLC = true; // is leaf the left child of par
    bool isleafchildLC; // is leafchild the left child of leaf
    
    
    leaf = (node_t *)get_addr_for_reading(leafPointerWord);
    if(key < leaf->key){
      leafchildPointerWord = leaf->child.AO_val1;
        isleafchildLC = true;
    }
    else{
        leafchildPointerWord = leaf->child.AO_val2;
        isleafchildLC = false;
    }
    
    leafchild = (node_t *)get_addr_for_comparing(leafchildPointerWord);
    
  while(leafchild != NULL){
        if(!is_marked(leafPointerWord)){
            gpar = par;
            parentPointerWord = leafPointerWord;
            isparLC = isleafLC;
        }
        
        par = leaf;
        leafPointerWord = leafchildPointerWord;
        isleafLC = isleafchildLC;
        
        leaf = (node_t*)get_addr_for_reading((AO_t)leafchild);
        
        if(key < leaf->key){
            leafchildPointerWord = leaf->child.AO_val1;
            isleafchildLC = true;
        }
        else{
            leafchildPointerWord = leaf->child.AO_val2;
            isleafchildLC = false;
        }    
        
        leafchild = (node_t *)get_addr_for_comparing(leafchildPointerWord);
        
    }
            
    if( !is_flagged(leafPointerWord) || (leaf != flaggedLeaf) ){
        // operation has been completed by another process.
        return NULL;        
     }
    
    seekRecord_t * R = data->ssr;
    
    R->leafKey = leaf->key;
        
    R->parent = par;
    
    R->pL = leafPointerWord;
    
    R->isLeftL = isleafLC;
    
    
    R->lum = gpar;
    R->lumC = parentPointerWord;    
    R->isLeftUM = isparLC;
    
  return R;
}

bool search(thread_data_t * data, long key){
    EpochStart(data->epoch);
    node_t * cur = (node_t *)get_addr_for_reading(data->rootOfTree->child.AO_val1);
    long lastKey;    
    while(cur != NULL){
      lastKey = cur->key;
        cur = (key < lastKey? (node_t *)get_addr_for_reading(cur->child.AO_val1): (node_t *)get_addr_for_reading(cur->child.AO_val2));
    }

#ifdef BUFFERING_ON
    cache_scan(data->buffer, key);
#endif
    EpochEnd(data->epoch);
  	return (key == lastKey);
}


//-------------------------------------------------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------------------------------------------------

int help_conflicting_operation (thread_data_t * data, seekRecord_t * R){

    if(is_flagged(R->pL)){
        // leaf node is flagged for deletion by another process.
        //1. mark sibling of leaf node for deletion and then read its contents.
        AO_t pS;
        
        if(R->isLeftL){
            // L is the left child of P
            mark_Node(&R->parent->child.AO_val2);
            pS = R->parent->child.AO_val2;
            
        }
        else{
            mark_Node(&R->parent->child.AO_val1);
            pS = R->parent->child.AO_val1;
        }
        
        // 2. Execute cas on the last unmarked node to remove the 
        // if pS is flagged, propagate it. 
        AO_t newWord;
        
        if(is_flagged(pS)){
            newWord = create_child_word((node_t *)get_addr_for_comparing(pS), UNMARK, FLAG);    
        }
        else{
            newWord = create_child_word((node_t *)get_addr_for_comparing(pS), UNMARK, UNFLAG);
        }
        
        int result;
        EpochDeclareUnlinkNode(data->epoch, (void*)R->lumC, sizeof(node_t));
        if(R->isLeftUM){
             // result = atomic_cas_full(&R->lum->child.AO_val1, R->lumC, newWord);
             result = cache_try_link_and_add(data->buffer, R->leafKey, (VPVOID*)&R->lum->child.AO_val1, (VPVOID)R->lumC, (VPVOID)newWord);
        }
        else{
             // result = atomic_cas_full(&R->lum->child.AO_val2, R->lumC, newWord);
             result = cache_try_link_and_add(data->buffer, R->leafKey, (VPVOID*)&R->lum->child.AO_val2, (VPVOID)R->lumC, (VPVOID)newWord);
        }
        return result; 
        
    }
    else{
        // leaf node is marked for deletion by another process.
        // Note that leaf is not flagged, as it will be taken care of in the above case.
        
        AO_t newWord;
        
        if(is_flagged(R->pL)){
            newWord = create_child_word((node_t *)get_addr_for_comparing(R->pL), UNMARK, FLAG);
        }
        else{
            newWord = create_child_word((node_t *)get_addr_for_comparing(R->pL), UNMARK, UNFLAG);
        }
        
        int result;
        EpochDeclareUnlinkNode(data->epoch, (void*)R->lumC, sizeof(node_t));
        if(R->isLeftUM){
             // result = atomic_cas_full(&R->lum->child.AO_val1, R->lumC, newWord);
             result = cache_try_link_and_add(data->buffer, R->leafKey, (VPVOID*)&R->lum->child.AO_val1, (VPVOID)R->lumC, (VPVOID)newWord);
        }
        else{
            // result = atomic_cas_full(&R->lum->child.AO_val2, R->lumC, newWord);
            result = cache_try_link_and_add(data->buffer, R->leafKey, (VPVOID*)&R->lum->child.AO_val2, (VPVOID)R->lumC, (VPVOID)newWord);
        }
        
    return result; 
    }    
        
}

//-------------------------------------------------------------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------------------------------------------------------------


int inject(thread_data_t * data, seekRecord_t * R, int op){
    
        // pL is free
        
        //1. Flag L
        
        AO_t newWord = create_child_word((node_t *)get_addr_for_comparing(R->pL),UNMARK,FLAG);
        
        int result; 
        
        if(R->isLeftL){
            // result = atomic_cas_full(&R->parent->child.AO_val1, R->pL, newWord);
            result = cache_try_link_and_add(data->buffer, R->leafKey, (VPVOID*)&R->parent->child.AO_val1, (VPVOID)R->pL, (VPVOID)newWord);
            
        }
        else{
            // result = atomic_cas_full(&R->parent->child.AO_val2, R->pL, newWord);
            result = cache_try_link_and_add(data->buffer, R->leafKey, (VPVOID*)&R->parent->child.AO_val2, (VPVOID)R->pL, (VPVOID)newWord);
        }
        
        return result;
}

bool insert(thread_data_t * data, long key){

	  
	int injectResult;
	int fasttry = 0;    
    EpochStart(data->epoch);

    while(true){
        seekRecord_t * R = insseek(data, key, INS);
        fasttry++;
        if(R == NULL){
            if(fasttry == 1){
#ifdef BUFFERING_ON
                cache_scan(data->buffer, key);
#endif
                EpochEnd(data->epoch);
                return false;
            }
            else{
            	EpochEnd(data->epoch);
                return true;
            }    
        }
        
        if(!is_free(R->pL)){
          help_conflicting_operation(data, R);
            continue;
        }
        
        // key not present in the tree. Insert        
        injectResult = perform_one_insert_window_operation(data, R, key);
        
        if(injectResult == 1){
            // Operation injected and executed
            EpochEnd(data->epoch);
            return true;
        }
        
    }
    // execute insert window operation.    
} 

bool delete_node(thread_data_t * data, long key){
    EpochStart(data->epoch);
    int injectResult;
    
    while(true){
        seekRecord_t * R = delseek(data, key, DEL);
        
        if(R == NULL){
#ifdef BUFFERING_ON
            cache_scan(data->buffer, key);
#endif
            EpochEnd(data->epoch);
            return false;
        }
        
        // key is present in the tree. Inject operation into the tree
        
        if(!is_free(R->pL)){
            
            help_conflicting_operation(data, R);
            continue;
        }
        
        injectResult = inject(data, R, DEL);

        if(injectResult == 1){
            // Operation injected 
            
            data->numActualDelete++;
            
            int res = perform_one_delete_window_operation(data, R, key);
            
            if(res == 1){
                // reclaim parent and leaf nodes
                EpochReclaimObject(data->epoch, (node_t *)get_addr_for_reading(R->pL), NULL, NULL, finalize_node);
                EpochReclaimObject(data->epoch, (node_t *)(R->parent), NULL, NULL, finalize_node);
                // operation successfully executed.
                EpochEnd(data->epoch);
                return true;
            }
            else{
                // window transaction could not be executed.
                // perform secondary seek.
                
                while(true){
                    R = secondary_seek(data, key, R);
                    
                    if(R == NULL){
                        // flagged leaf not found. Operation has been executed by some other process.
#ifdef BUFFERING_ON
                        cache_scan(data->buffer, key);
#endif
                        EpochEnd(data->epoch);
                        return false;
                    }
                    
                    res = perform_one_delete_window_operation(data, R, key);
                    
                    if(res == 1){
                        // reclaim parent and leaf nodes
                        EpochReclaimObject(data->epoch, (node_t *)get_addr_for_reading(R->pL), NULL, NULL, finalize_node);
                        EpochReclaimObject(data->epoch, (node_t *)(R->parent), NULL, NULL, finalize_node);
                    	EpochEnd(data->epoch);
                        return true;
                    }
                }
            }
        }
        // otherwise, operation was not injected. Restart.
    }
}

int is_reachable(node_t* root, void* address) {

    if (root == NULL) { 
        return 0 ;
    }

    if ((void*) root == address ) {
        return 1;
    }

    return (is_reachable((node_t *)get_addr_for_reading(root->child.AO_val1), address) || is_reachable((node_t *)get_addr_for_reading(root->child.AO_val2), address));
}

void recover(thread_data_t * data, active_page_table_t** page_buffers, int num_page_buffers) {

    int i;
    // TODO: return data structure to consistent state

    // go over all the page tables
    size_t j;
    size_t k;
    size_t page_size;
    size_t nodes_per_page;

    page_descriptor_t* crt;
    size_t num_pages;

    for (i = 0; i < num_page_buffers; i++) {
        page_size = page_buffers[i]->page_size; //TODO: now assuming all the pages in the buffer have one size; change this? (given that in the NV heap we basically just use one page size (except the bottom level), should be fine)
        num_pages = page_buffers[i]->last_in_use;
        crt = page_buffers[i]->pages;
        for (j = 0; j < num_pages; j++) {
                if (crt[j].page != NULL) {
                    void * crt_address = crt[j].page;
                    nodes_per_page = page_size / sizeof(node_t);
                    for (k = 0; k < nodes_per_page; k++) {
                        void * node_address = (void*)((UINT_PTR)crt_address + (sizeof(node_t)*k));
                        if (!NodeMemoryIsFree(node_address)) {
                            if (!is_reachable(data->rootOfTree, node_address)) {

                                MarkNodeMemoryAsFree(node_address); //if a node is not reachable but its memory is marked as allocated, need to free the node
          
                            }
                        }
                    }
                }
            }
        destroy_active_page_table(page_buffers[i]);
    }
}

