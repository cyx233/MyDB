#include <iostream>
#include <recordmanager/database.h>
int main(int, char **)
{
    Database *a = new Database();
    a->create("testdb");
    std::cout << "Hello, world!\n";
}
