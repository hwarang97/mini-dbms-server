#include "bptree.h"

#include <stdlib.h>

/* search 와 insert 모두 같은 규칙으로 root 에서 leaf 까지 내려간다. */
static BPTreeNode *bptree_find_leaf(BPTree *tree, int key)
{
    BPTreeNode *node;
    int index;

    if (!tree || !tree->root) {
        return NULL;
    }

    node = tree->root;
    while (node && !node->is_leaf) {
        index = 0;
        while (index < node->key_count && key >= node->keys[index]) {
            index++;
        }
        node = node->children[index];
    }

    return node;
}

/* 리프 안에서 key 가 들어갈 정렬 위치를 찾고, 이미 있으면 found 를 1로 돌려준다. */
static int bptree_find_key_index(BPTreeNode *node, int key, int *found)
{
    int index = 0;

    if (found) {
        *found = 0;
    }

    while (index < node->key_count && node->keys[index] < key) {
        index++;
    }

    if (found && index < node->key_count && node->keys[index] == key) {
        *found = 1;
    }

    return index;
}

/* 부모 안에서 기준이 되는 왼쪽 child 의 슬롯을 찾아야 separator key 를 끼워 넣을 수 있다. */
static int bptree_find_child_slot(BPTreeNode *parent, BPTreeNode *child)
{
    int index;

    if (!parent || !child) {
        return -1;
    }

    for (index = 0; index <= parent->key_count; index++) {
        if (parent->children[index] == child) {
            return index;
        }
    }

    return -1;
}

static void bptree_insert_into_leaf(BPTreeNode *leaf, int insert_index, int key, void *value)
{
    int move;

    for (move = leaf->key_count; move > insert_index; move--) {
        leaf->keys[move] = leaf->keys[move - 1];
        leaf->values[move] = leaf->values[move - 1];
    }

    leaf->keys[insert_index] = key;
    leaf->values[insert_index] = value;
    leaf->key_count++;
}

static int bptree_create_new_root(BPTree *tree, BPTreeNode *left, int key, BPTreeNode *right)
{
    BPTreeNode *new_root;

    new_root = bptree_create_node(0);
    if (!new_root) {
        return -1;
    }

    new_root->key_count = 1;
    new_root->keys[0] = key;
    new_root->children[0] = left;
    new_root->children[1] = right;

    left->parent = new_root;
    right->parent = new_root;
    tree->root = new_root;

    return 0;
}

static void bptree_insert_into_internal(BPTreeNode *node,
                                        int left_child_index,
                                        int key,
                                        BPTreeNode *right_child)
{
    int move;

    for (move = node->key_count; move > left_child_index; move--) {
        node->keys[move] = node->keys[move - 1];
    }

    for (move = node->key_count + 1; move > left_child_index + 1; move--) {
        node->children[move] = node->children[move - 1];
    }

    node->keys[left_child_index] = key;
    node->children[left_child_index + 1] = right_child;
    right_child->parent = node;
    node->key_count++;
}

static int bptree_insert_into_parent(BPTree *tree, BPTreeNode *left, int key, BPTreeNode *right);

/* leaf 가 넘치면 오른쪽 leaf 를 새로 만들고, 오른쪽 첫 key 를 부모에 올린다. */
static int bptree_split_leaf(BPTree *tree, BPTreeNode *leaf, int key, void *value)
{
    int temp_keys[BPTREE_MAX_KEYS + 1];
    void *temp_values[BPTREE_MAX_KEYS + 1];
    BPTreeNode *new_leaf;
    int found;
    int insert_index;
    int split_index;
    int index;

    insert_index = bptree_find_key_index(leaf, key, &found);
    if (found) {
        leaf->values[insert_index] = value;
        return 0;
    }

    for (index = 0; index < leaf->key_count; index++) {
        temp_keys[index] = leaf->keys[index];
        temp_values[index] = leaf->values[index];
    }

    for (index = leaf->key_count; index > insert_index; index--) {
        temp_keys[index] = temp_keys[index - 1];
        temp_values[index] = temp_values[index - 1];
    }

    temp_keys[insert_index] = key;
    temp_values[insert_index] = value;

    new_leaf = bptree_create_node(1);
    if (!new_leaf) {
        return -1;
    }

    split_index = (BPTREE_MAX_KEYS + 1) / 2;

    leaf->key_count = split_index;
    for (index = 0; index < split_index; index++) {
        leaf->keys[index] = temp_keys[index];
        leaf->values[index] = temp_values[index];
    }
    for (index = split_index; index < BPTREE_MAX_KEYS; index++) {
        leaf->keys[index] = 0;
        leaf->values[index] = NULL;
    }

    new_leaf->key_count = (BPTREE_MAX_KEYS + 1) - split_index;
    for (index = 0; index < new_leaf->key_count; index++) {
        new_leaf->keys[index] = temp_keys[split_index + index];
        new_leaf->values[index] = temp_values[split_index + index];
    }

    new_leaf->parent = leaf->parent;
    new_leaf->next = leaf->next;
    leaf->next = new_leaf;

    return bptree_insert_into_parent(tree, leaf, new_leaf->keys[0], new_leaf);
}

/* internal node 가 넘치면 가운데 key 하나를 부모로 올리고 좌우로 다시 나눈다. */
static int bptree_split_internal(BPTree *tree,
                                 BPTreeNode *node,
                                 int left_child_index,
                                 int key,
                                 BPTreeNode *right_child)
{
    int temp_keys[BPTREE_MAX_KEYS + 1];
    BPTreeNode *temp_children[BPTREE_MAX_CHILDREN + 1];
    BPTreeNode *new_internal;
    int total_keys;
    int promote_index;
    int promote_key;
    int index;

    for (index = 0; index < node->key_count; index++) {
        temp_keys[index] = node->keys[index];
    }

    for (index = 0; index <= node->key_count; index++) {
        temp_children[index] = node->children[index];
    }

    for (index = node->key_count; index > left_child_index; index--) {
        temp_keys[index] = temp_keys[index - 1];
    }
    temp_keys[left_child_index] = key;

    for (index = node->key_count + 1; index > left_child_index + 1; index--) {
        temp_children[index] = temp_children[index - 1];
    }
    temp_children[left_child_index + 1] = right_child;

    total_keys = node->key_count + 1;
    promote_index = total_keys / 2;
    promote_key = temp_keys[promote_index];

    new_internal = bptree_create_node(0);
    if (!new_internal) {
        return -1;
    }

    node->key_count = promote_index;
    for (index = 0; index < node->key_count; index++) {
        node->keys[index] = temp_keys[index];
    }
    for (index = node->key_count; index < BPTREE_MAX_KEYS; index++) {
        node->keys[index] = 0;
    }
    for (index = 0; index <= node->key_count; index++) {
        node->children[index] = temp_children[index];
        if (node->children[index]) {
            node->children[index]->parent = node;
        }
    }
    for (index = node->key_count + 1; index < BPTREE_MAX_CHILDREN; index++) {
        node->children[index] = NULL;
    }

    new_internal->key_count = total_keys - promote_index - 1;
    for (index = 0; index < new_internal->key_count; index++) {
        new_internal->keys[index] = temp_keys[promote_index + 1 + index];
    }
    for (index = 0; index <= new_internal->key_count; index++) {
        new_internal->children[index] = temp_children[promote_index + 1 + index];
        if (new_internal->children[index]) {
            new_internal->children[index]->parent = new_internal;
        }
    }
    new_internal->parent = node->parent;

    return bptree_insert_into_parent(tree, node, promote_key, new_internal);
}

static int bptree_insert_into_parent(BPTree *tree, BPTreeNode *left, int key, BPTreeNode *right)
{
    BPTreeNode *parent;
    int left_child_index;

    parent = left->parent;
    if (!parent) {
        return bptree_create_new_root(tree, left, key, right);
    }

    left_child_index = bptree_find_child_slot(parent, left);
    if (left_child_index < 0) {
        return -1;
    }

    if (parent->key_count < BPTREE_MAX_KEYS) {
        bptree_insert_into_internal(parent, left_child_index, key, right);
        return 0;
    }

    return bptree_split_internal(tree, parent, left_child_index, key, right);
}

/* 노드를 재귀적으로 지운다.
 * 내부 노드는 자식들을 먼저 지우고,
 * 마지막에 자기 자신을 지운다.
 */
static void bptree_destroy_node(BPTreeNode *node)
{
    int i;

    if (!node) {
        return;
    }

    if (!node->is_leaf) {
        for (i = 0; i <= node->key_count; i++) {
            bptree_destroy_node(node->children[i]);
        }
    }

    free(node);
}

/* 노드 하나를 0으로 초기화해서 만든다.
 * calloc 을 써서 key, child, value 포인터들이 모두 비어 있는 상태로 시작한다.
 */
BPTreeNode *bptree_create_node(int is_leaf)
{
    BPTreeNode *node = (BPTreeNode *)calloc(1, sizeof(BPTreeNode));

    if (!node) {
        return NULL;
    }

    node->is_leaf = is_leaf;
    return node;
}

/* 트리를 만들 때는 가장 단순한 상태로 시작한다.
 * 루트 하나만 있고, 그 루트는 곧 리프 노드다.
 */
BPTree *bptree_create(void)
{
    BPTree *tree = (BPTree *)calloc(1, sizeof(BPTree));

    if (!tree) {
        return NULL;
    }

    tree->root = bptree_create_node(1);
    if (!tree->root) {
        free(tree);
        return NULL;
    }
    tree->first_leaf = tree->root;

    return tree;
}

/* 트리 해제 함수.
 * root 아래에 달린 모든 노드를 지운 뒤 트리 구조체도 함께 지운다.
 */
void bptree_destroy(BPTree *tree)
{
    if (!tree) {
        return;
    }

    bptree_destroy_node(tree->root);
    free(tree);
}

/* search 의 실제 동작.
 * 1. 먼저 key 가 있어야 할 리프 노드를 찾는다.
 * 2. 그 리프 노드 안에서 key 를 앞에서부터 비교한다.
 * 3. 같은 key 를 찾으면 연결된 값을 돌려주고, 없으면 NULL 을 돌려준다.
 */
void *bptree_search(BPTree *tree, int key)
{
    BPTreeNode *node;
    int i;

    node = bptree_find_leaf(tree, key);
    if (!node) {
        return NULL;
    }

    for (i = 0; i < node->key_count; i++) {
        if (node->keys[i] == key) {
            return node->values[i];
        }
    }

    return NULL;
}

/* insert 는 search 와 같은 leaf 탐색 경로를 공유한다.
 * 리프에 자리가 있으면 바로 넣고, 넘치면 leaf split -> parent 반영으로 올라간다.
 */
int bptree_insert(BPTree *tree, int key, void *value)
{
    BPTreeNode *leaf;
    int found;
    int insert_index;

    if (!tree || !tree->root) {
        return -1;
    }

    leaf = bptree_find_leaf(tree, key);
    if (!leaf) {
        return -1;
    }

    insert_index = bptree_find_key_index(leaf, key, &found);
    if (found) {
        leaf->values[insert_index] = value;
        return 0;
    }

    if (leaf->key_count < BPTREE_MAX_KEYS) {
        bptree_insert_into_leaf(leaf, insert_index, key, value);
        return 0;
    }

    return bptree_split_leaf(tree, leaf, key, value);
}

int bptree_insert_unique(BPTree *tree, int key, void *value)
{
    BPTreeNode *leaf;
    int found;
    int insert_index;

    if (!tree || !tree->root) {
        return -1;
    }

    leaf = bptree_find_leaf(tree, key);
    if (!leaf) {
        return -1;
    }

    insert_index = bptree_find_key_index(leaf, key, &found);
    if (found) {
        return 1;
    }

    if (leaf->key_count < BPTREE_MAX_KEYS) {
        bptree_insert_into_leaf(leaf, insert_index, key, value);
        return 0;
    }

    return bptree_split_leaf(tree, leaf, key, value);
}
