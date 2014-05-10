/*************************************************************************
 * sll.c
 *
 * Implements a simple singly-linked list structure for ints.
 ************************************************************************/

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

// the size of our test list: feel free to adjust as you wish!
#define TEST_SIZE 10

typedef struct node
{
    // the value to store in this node
    int i;

    // the link to the next node in the list
    struct node* next;
}
node;

// declare the first node of our list (as a global variable)
node* first = NULL;

/**
 * Returns the length of the list.
 */
int length(void)
{
    // Initialize counter
    int i = 0;
    
    // Set n = to first; loop till n == NULL; update n to the next element
    for (node* n = first; n != NULL; n = n->next)
        i++;
    
    // Return counter
    return i;
}

/**
 * Returns true if a node in the list contains the value i and false
 * otherwise.
 */
bool contains(int i)
{
    // Iterate through linked list
    for (node* n = first; n != NULL; n = n->next)
    {
        // If current element is what we're looking for, return true
        if (n->i == i)
            return true;
    }
    
    // Return false if the input was not found
    return false;
}

/**
 * Helper function: creates a node on the heap. Returns pointer to new node,
 * initialized with input value i.
 */

node* create_node(int i)
{
    // Malloc new node
	node* new_node = malloc(sizeof(node));
    
    // Return NULL if malloc failed
    if (new_node == NULL)
        return NULL;
    
    // Initialize new node with input value i, and make sure it points to NULL
    new_node->i = i;
    new_node->next = NULL;
    
    return new_node;
}

/**
 * Puts a new node containing i at the front (head) of the list.
 */
void prepend(int i)
{
    // Keep track of where first is pointing now
    node* original_first = first;
    
    // Create a new node on the heap
    node* new_node = create_node(i);
    
    // Update 'first' variable to point to new node
    first = new_node;
    
    // Update new node to point to where first used to point
    new_node->next = original_first;
}

/**
 * Puts a new node containing i at the end (tail) of the list.
 */
void append(int i)
{
    // If the list is empty, we can just insert at the front of the list
    if (first == NULL)
    {
        prepend(i);
        return;
    }
    
    // Keep track of the element before the one we are currently examining
    node* prev = NULL;
    
    // Traverse linked list till we reach the end, keeping track of the previous element
    for (node* ptr = first; ptr != NULL; ptr = ptr->next)
        prev = ptr;
    
    // Create a new node on the heap
    node* new_node = create_node(i);
    
    // Set the node at the end of the linked list to point to the new node
    prev->next = new_node;
}

/**
 * Puts a new node containing i at the appropriate position in a list
 * sorted in ascending order.
 */
void insert_sorted(int i)
{
    // If the list is empty or the first element is larger, we can just prepend
    if (first == NULL || first->i >= i)
    {
        prepend(i);
        return;
    }
    
    // Keep track of the element before the one we are currently examining
    node* prev = first;
    
    // Traverse the linked list
    for (node* ptr = first; ptr != NULL; ptr = ptr->next)
    {
        // If the current node's value is greater than the one we're inserting...
        if (ptr->i >= i)
        {
            // Create a new node
            node* new_node = create_node(i);
            // Set the previous node to point to the new one
            prev->next = new_node;
            // Set thew ne node to point to the current one
            new_node->next = ptr;
            return;
        }
        // Update the 'prev' pointer before moving to next element
	    prev = ptr;
    }
    
    // If we didn't find any greater values, insert to the end of the list
    append(i);
}