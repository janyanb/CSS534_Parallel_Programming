
#include <iostream>  // cout
#include <stdlib.h>  // rand
#include <math.h>    // sqrt, pow
#include <omp.h>     // OpenMP
#include <string.h>  // memset
#include <iomanip>
#include "Timer.h"
#include "Trip.h"
#include <algorithm>

#define CHROMOSOMES    50000 // 50000 different trips
#define CITIES         36    // 36 cities = ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789
#define TOP_X          25000 // top optimal 25%
#define MUTATE_RATE    5    // optimal 50%

using namespace std;

string cities = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
string citiesComplement = "9876543210ZYXWVUTSRQPONMLKJIHGFEDCBA";


//sorting function which sorts the trip array in ascending order based on fitness
bool sortingFunction(const Trip &a, const Trip &b){
  return a.fitness < b.fitness;
}

int getCityIndex(char city)
{
    return (city >= 'A') ? city - 'A' : city - '0' + 26;
}

/*
 * Evaluates each trip (or chromosome) and sort them out
 */
void evaluate( Trip trip[CHROMOSOMES], double distanceBtwCities[CITIES][CITIES] ) {

#pragma omp parallel for shared(trip,distanceBtwCities)                                 // parallelises the calculatio of distance for a set of chromosomes based on number of threads
  for(int i = 0; i < CHROMOSOMES; i++){
    double totalDistance = 0.0;
      for(int j = 0; j < CITIES -1; j++){
	      int City1index = cities.find(trip[i].itinerary[j]); //returns the position of the city on the distance matrix
	      int City2index = cities.find(trip[i].itinerary[j + 1]);

	      totalDistance += distanceBtwCities[City1index][City2index];
      }
	    trip[i].fitness = totalDistance;
	    //cout<< "Trip "<< i << "total distance:" << totalDistance << endl;
    }
  sort(trip, trip + CHROMOSOMES, sortingFunction);
 /* for(int i = 0; i < CHROMOSOMES; i ++){
      cout << trip[i].fitness << endl;
  }*/
}

/*
 * Generates new TOP_X offsprings from TOP_X parents.
 * Noe that the i-th and (i+1)-th offsprings are created from the i-th and (i+1)-th parents
 */
void crossover( Trip parents[TOP_X], Trip offsprings[TOP_X], double distanceBtwCities[CITIES][CITIES] ) {

  #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < TOP_X; i += 2)
    {
        bool visited[91] = {false};
        char offSpring1[CITIES];
        char offSpring2[CITIES];

        // get the first city of parent i and mark as visited
        offSpring1[0] = parents[i].itinerary[0];
        visited[offSpring1[0]] = true;

        // get parent itineraries
        string parent1 = parents[i].itinerary;
        string parent2 = parents[i + 1].itinerary;

        for (int j = 0; j < (CITIES - 1); j++)
        {
            // get the index of city after the current city in parent1 and parent2
            int candidateIn1 = (parent1.find(offSpring1[j]) + 1) % CITIES;
            int candidateIn2 = (parent2.find(offSpring1[j]) + 1) % CITIES;

            char city1 = parent1[candidateIn1];
            char city2 = parent2[candidateIn2];

            // get their indexes so that we can get the distance from distance matrix
            int currentCityIndex = getCityIndex(offSpring1[j]);
            int nextCityIndex1 = getCityIndex(city1);
            int nextCityIndex2 = getCityIndex(city2);

            float disA = 0.0;
            float disB = 0.0;

            // if both the cities are not yet visited
            if (!visited[city1] && !visited[city2])
            {
                disA = distanceBtwCities[currentCityIndex][nextCityIndex1];
                disB = distanceBtwCities[currentCityIndex][nextCityIndex2];
                offSpring1[j + 1] = (disA <= disB) ? parent1[candidateIn1] : parent2[candidateIn2];
            }
            else if (!visited[city1])
            {
                //disA = distanceBtwCities[currentCityIndex][nextCityIndex1];
                offSpring1[j + 1] = parent1[candidateIn1];
            }
            else if (!visited[city2])
            {
                //disB = distanceBtwCities[currentCityIndex][nextCityIndex2];
                offSpring1[j + 1] = parent2[candidateIn2];
            }
            else
            {
                for (int k = 0; k < CITIES; k++)
                {
                    // if both the cities are visited, get the unvisited city from parent1
                    if (!visited[parents[i].itinerary[k]])
                    {
                        offSpring1[j + 1] = parent1[k];
                        break;
                    }
                }
            }

            visited[offSpring1[j + 1]] = true; // to make sure its visited
        }

        // once child[i] is ready, create child[i+1] by complementing the child1
        for (int j = 0; j < CITIES; ++j)
        {
            int complemented_index = cities.find(offSpring1[j]);
            offSpring2[j] = citiesComplement[complemented_index];
        }

        // store into offsprings array
        strncpy(offsprings[i].itinerary, offSpring1, CITIES);
        strncpy(offsprings[i + 1].itinerary, offSpring2, CITIES);
    }

    /*
    for(int i = 0; i< TOP_X; i++){
      cout << offsprings[i].itinerary << endl;
    } */
}

/*
 * Mutate a pair of genes in each offspring.
 */
void mutate( Trip offsprings[TOP_X] ) {

  for(int i = 0; i < TOP_X ; i++){

//generates a random number between 0-100 to determine mutation chances 
    int mutateRate = rand() % 100;
    if(mutateRate < MUTATE_RATE){

      int city1Index = rand() % 36;
      int city2Index = rand() % 36;

     /* do {
        city2Index = rand() % 36;              
      } while(city1Index == city2Index);  //ensures city1Index and city2index are not the same. */

      char city1 = offsprings[i].itinerary[city1Index];  
      offsprings[i].itinerary[city1Index] = offsprings[i].itinerary[city2Index];
      offsprings[i].itinerary[city2Index] = city1;    

    }

  }

}

void calculateDistance( int coordinates[CITIES][2], double distanceBtwCities[CITIES][CITIES]) {
//calculates the distance between all [36][36] cities for faster look-up
#pragma omp parallel for
  for(int i = 0; i<CITIES; i++){
    for(int j = 0; j<CITIES; ++j){

      float X_distance = coordinates[j][0] - coordinates[i][0];
      float Y_distance = coordinates[j][1] - coordinates[i][1];

      double eucledian_distance = sqrt((X_distance * X_distance) + (Y_distance * Y_distance));
      distanceBtwCities[i][j]= eucledian_distance;
      }
  }
  /*
  for(int i = 0; i < CITIES; i++){
    for(int j = 0; j <CITIES; ++j){
      cout<< distanceBtwCities[i][j];
    }
    cout<< endl;
    }*/

} 


