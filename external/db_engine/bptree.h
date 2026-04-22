#ifndef BPTREE_H
#define BPTREE_H

#ifndef BPTREE_ORDER
#define BPTREE_ORDER 64
#endif
#define BPTREE_MAX_KEYS (BPTREE_ORDER - 1)
#define BPTREE_MAX_CHILDREN (BPTREE_ORDER)

/* B+ Tree 의 한 노드.
 * 내부 노드와 리프 노드가 같은 구조를 쓰고,
 * is_leaf 값으로 현재 노드 종류를 구분한다.
 */
typedef struct BPTreeNode
{
    int is_leaf;                         /* 1이면 리프 노드, 0이면 내부 노드 */
    int key_count;                       /* 현재 노드에 실제로 들어 있는 key 개수 */
    int keys[BPTREE_MAX_KEYS];           /* key 들은 항상 작은 값부터 차례대로 저장 */
    struct BPTreeNode *children[BPTREE_MAX_CHILDREN]; /* 내부 노드일 때 아래 자식들을 가리킴 */
    void *values[BPTREE_MAX_KEYS];       /* 리프 노드일 때 key 에 대응하는 값 */
    struct BPTreeNode *parent;           /* split 때 위로 올라가기 위해 부모를 기억 */
    struct BPTreeNode *next;             /* 리프 노드끼리 오른쪽으로 연결 */
} BPTreeNode;

/* 트리 전체를 나타내는 구조체.
 * search 는 root 에서 시작하고,
 * 나중에 range scan 이 필요할 때는 first_leaf 부터 순서대로 볼 수 있다.
 */
typedef struct BPTree
{
    BPTreeNode *root;       /* 탐색을 시작하는 가장 위 노드 */
    BPTreeNode *first_leaf; /* 가장 왼쪽 리프 노드 */
} BPTree;

/* 사이클 1에서 구조체를 확정하고, 사이클 2에서 insert/split 을 붙인다. */
BPTree *bptree_create(void);
void bptree_destroy(BPTree *tree);
BPTreeNode *bptree_create_node(int is_leaf);
void *bptree_search(BPTree *tree, int key);
int bptree_insert(BPTree *tree, int key, void *value);
int bptree_insert_unique(BPTree *tree, int key, void *value);

#endif
