#include<iostream>
#include<fstream>
#include<algorithm>
#include<iterator>
#include<assert.h>
#include <dirent.h>

void getListOfFiles(std::string directory, std::vector<std::string>* fileList)
{
  DIR *dp;
  struct dirent *dirp;

  if ((dp = opendir(directory.c_str())) == NULL)
    return;

  //Also check . and .. are not included in the filename
  std::string forbiddenFilename1 = ".";
  std::string forbiddenFilename2 = "..";
  while ((dirp = readdir(dp)) != NULL)
  {
    std::string fname = std::string(dirp->d_name);
    if (fname.compare(forbiddenFilename1) != 0 && fname.compare(forbiddenFilename2) != 0)
    {
      fileList->push_back(fname);
    }
  }
  closedir(dp);
}


int main(int argc, char** argv) {
    std::string inputFolder = argv[1];
    int num = atoi(argv[2]); 
    if(argc !=3){
      std::cout<<"Usage <inputFolder> <nparts> will give you count in each part" <<std::endl;
      return 0;
      }
     
     std::vector<std::string> fileList;
     getListOfFiles(inputFolder, &fileList);

     std::cout << "Number of files: " << fileList.size() << std::endl;

     unsigned fileId = 0;
     unsigned line_count = 0;
     while(fileId < fileList.size()) {
          std::string fname = inputFolder + "/" + fileList.at(fileId);
     	  std::cout <<"\n*************************************************\n \n";
     	  std::cout << "\nFileName: " <<fname;

	  std::ifstream infile(fname.c_str());
//	  ASSERT_WITH_MESSAGE(infile.is_open(), fname.c_str());
	  std::string line;
   	  while(std::getline(infile, line)) {
		line_count++;
		}
	    std::cout<<"\nTotal lines " << line_count  << "\n";
	    fileId++;
	    line_count=0;
        }
    return 0;
}

