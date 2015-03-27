#include<iostream>
#include<sstream>
#include<map>
#include<string>
#include<string.h>
#include<stdio.h>
#include<algorithm>
#include<stdlib.h>
#include<deque>
#include<fstream>
#include<cstring>
#include<typeinfo>
#include<vector>
#include<dirent.h>
using namespace std;

typedef struct metadata
{
    string name;
    int num;
    int size;
    int records[100];

    string attribute[100];
    map<string,int> unique[10];
    map<string,int>final;
}metadata;
typedef struct joining
{
    string tab[2];
    string att[2];
    int rows[2],order;
    double cost;
}joining;

typedef struct page
{
    int id;
    int modified;
    char *data;
}page;

typedef struct query
{
    string type;
    string columns;
    string tables;
    string condition;
    string conditions[10];
    string groupby;
    string having;
    string join_conditions[10];   
    vector<string> orderby;
    int orderasc[100];
    int ordertype[100];
    string attributes;
    string distinct;
    int flags[4];
}query;

typedef vector<page> vp;
typedef vector<vp> vvp;
typedef vector<string> vs;
typedef vector<vs> vvs;
typedef vector<vvs> vvvs;

struct comparefun
{
    comparefun(int *idx, int *bit,int *type,int n){this->idx=idx;this->bit=bit;this->type=type;this->n=n;}
    bool operator ()(vector<string> a,vector<string> b)
    {
        int equal = 1,i=0;
        while( equal && i<n )
        {
            if( type[i] == 1 )
            {
                if( a[idx[i]].compare(b[idx[i]]) != 0 ){
                    equal = 0;
                    break;
                }
            }
            else
            {
                if ( ( atof(a[idx[i]].c_str())-atof(b[idx[i]].c_str())) != 0){
                    equal = 0; 
                    break;
                }
            }
            i++;
        }

        if( i == n ) i = n-1;

        if(bit[i]==1)
        {
            if(type[i]==1)
                return (a[idx[i]].compare(b[idx[i]])>=0)?true:false;
            else
                return ((atof(a[idx[i]].c_str())-atof(b[idx[i]].c_str()))>=0)?true:false;
        }
        else
        {
            if(type[i]==1)
                return (a[idx[i]].compare(b[idx[i]])>=0)?false:true;
            else
                return ((atof(a[idx[i]].c_str())-atof(b[idx[i]].c_str()))>=0)?false:true;
        }
    }
    int *idx;
    int *bit;
    int *type;
    int n;
};

class DBSystem
{
    public: 
        metadata tables[100];              // To store metadata for tables
        vvp pages;
        int mapping[2][10000];
        deque<page> LRU;
        int num_pages,pagesize,table_index,page_index;
        string filePath;
        joining join_tables[100];
        int join_index;
        // pagesize = LRU size
        // num_pages = Maximum number of pages
        // table_index = Number of tables
        // page_index = Number of pages
        // pages = Structure to store all data


        /////////////////////////////////////////////////////////////////////
        void readConfig(string configFilePath)
        {
            int i=0,k,idx;
            table_index = 0;
            string line,word[30];
            ifstream file1(configFilePath.c_str());
            while(getline(file1,line,'\n'))
            {
                stringstream ssin(line);
                while(ssin.good())                // File is read and parsed into words
                    ssin>>word[i++];
            }

            for(int k=0;k<i;k++)
            {
                if((word[k].compare("PAGE_SIZE"))==0)
                    pagesize=atoi(word[k+1].c_str());
                if((word[k].compare("NUM_PAGES"))==0)
                    num_pages=atoi(word[k+1].c_str());
                if((word[k].compare("PATH_FOR_DATA"))==0)
                    filePath = word[k+1];
                if((word[k].compare("BEGIN"))==0)
                {
                    k++;
                    tables[table_index].name = word[k++];

                    string tmp = filePath;
                    tmp.append(tables[table_index].name);
                    tables[table_index].name = tmp;

                    idx = 0;
                    while( word[k].compare("END")!=0 )
                    {
                        string tmp;
                        size_t pos = word[k].find(",");

                        ///////// Building metadata for each table ///////////////
                        tables[table_index].attribute[idx] = word[k].substr(0,pos);
                        tmp = word[k].substr(pos+1);

                        if( !tmp.compare("char") )
                            tables[table_index].records[idx] = 1;
                        else if( !tmp.compare("int") )
                            tables[table_index].records[idx] = 2;
                        else if( !tmp.compare("float") )
                            tables[table_index].records[idx] = 3;
                        else 
                            tables[table_index].records[idx] = 4;

                        k++;
                        idx++;
                        //////////////////////////////////////////////////////////
                    }
                    tables[table_index].num = idx;
                    table_index++;
                }

            }
        }
        ////////////////////////////////////////////////////////////////////////



        ////////////////////////////////////////////////////////////////////////
        void populateDBInfo()
        {
            int i,j,k,idx;
            page_index = 0;
            int remaining = pagesize;

            // Iterate through all tables
            for(i=0;i<table_index;i++)
            {
                int num = 0;
                vector<page> temp;
                temp.resize(1);
                temp[num].data = new char[pagesize]();
                temp[num].id = page_index;

                string line,record[10000];
                remaining = pagesize;

                // Get all records of current table
                int length;
                idx = 0;
                tables[i].name.append(".csv");
                ifstream file(tables[i].name.c_str());
                int ctr2=0;
                while(getline(file,line,'\n'))
                {
                    istringstream ss1(line);
                    string token1;
                    ctr2=0;
                    while(getline(ss1,token1,','))
                    {
                        tables[i].unique[ctr2][token1]=1;
                        ctr2++;
                    }
                    record[idx++] = line;
                }
                for(int yy=0;yy<ctr2;yy++)
                    tables[i].final[tables[i].attribute[yy]]=tables[i].unique[yy].size();

                mapping[0][page_index] = 0;

                // Iterate through the records
                for(j=0;j<idx;j++)
                {
                    length = record[j].length()+1;
                    // Check whether record can fit in the page
                    if( length < remaining )                        
                    {
                        strcat(temp[num].data,record[j].c_str());
                        strcat(temp[num].data,"+");
                        remaining -= length;
                    }
                    else
                    {
                        mapping[1][page_index] =  j;

                        page_index++;
                        num++;
                        temp.resize(temp.size()+1);
                        temp[num].data = new char[pagesize]();
                        temp[num].id = page_index;

                        mapping[0][page_index] = j;
                        remaining = pagesize;
                        j--;
                    }
                }


                mapping[1][page_index] =  j-1;
                page_index++;

                pages.push_back(temp);
                temp.clear();

            }
            /*                for(i=0;i<pages.size();i++)
                              {
                              k = pages[i].size();
                              for(j=0;j<k;j++)
                              cout << pages[i][j].id << endl << strlen(pages[i][j].data) << endl << pages[i][j].data << endl;
                              cout << endl;
                              }*/

            /*      for(int gg=0;gg<table_index;gg++)
                    {
                    cout<<"hheee    "<<tables[gg].final["s_id"]<<endl;
                    cout<<"hheee    "<<tables[gg].final["s_num"]<<endl;
                    cout<<"hheee    "<<tables[gg].final["s_order"]<<endl;
                    }*/
        }
        ////////////////////////////////////////////////////////////////////////



        ////////////////////////////////////////////////////////////////////////
        void getRecord(string tableName, int recordId)
        {
            ////////// Extracting relevant page ID
            int i,j,k;

            string tmp = filePath;
            tmp.append(tableName);
            tableName = tmp;
            tableName.append(".csv");

            for(i=0;i<table_index;i++)
                if( !tables[i].name.compare( tableName ) )
                    break;

            int l = 0;
            int r = pages[i].size() - 1;
            int mid = (l+r)/2;

            while( l < r )
            {
                int s = mapping[0][pages[i][mid].id];
                int t = mapping[1][pages[i][mid].id];

                if( recordId >= s && recordId <= t )
                    break;
                else if( recordId > t)
                    l = mid+1;
                else
                    r = mid-1;
                mid = (l+r)/2;
            }

            ////////// Searching for required page in LRU

            static int empty = num_pages;

            if( empty )
            {
                int flag = 0;
                j = LRU.size();
                for(k=0;k<j;k++)
                {
                    if( LRU[k].id == mid )
                    {
                        cout << "HIT\n";
                        page x = LRU[k];
                        flush(x);
                        LRU.erase(LRU.begin()+k);
                        LRU.push_front(x);
                        return;
                    }
                }

                j = pages[i].size();
                for(k=0;k<j;k++)
                    if( pages[i][k].id == mid )
                    {
                        LRU.push_front( pages[i][k] );
                        cout << "MISS: " << pages[i][k].id << endl;
                    }
                empty--;
                return;
            }
            else
            {
                int flag = 0;
                j = LRU.size();
                for(k=0;k<j;k++)
                    if( LRU[k].id == mid )
                    {
                        flag = 1;
                        break;
                    }

                if( !flag )
                {
                    j = pages[i].size();
                    for(k=0;k<j;k++)
                        if( pages[i][k].id == mid )
                            break;

                    page tmp = LRU[LRU.size() - 1];
                    flush(tmp);
                    int x = LRU[LRU.size()-1].id;
                    LRU.pop_back();
                    LRU.push_front( pages[i][k] );
                    cout << "MISS " << x << endl;
                }
                else
                {
                    page tmp = LRU[k];
                    flush(tmp);
                    LRU.erase(LRU.begin()+k);
                    LRU.push_front( tmp );

                    cout << "HIT\n";
                }
            }

        }
        ////////////////////////////////////////////////////////////////////////



        ////////////////////////////////////////////////////////////////////////
        void insertRecord(string tableName,string record)
        {

            int i,j,k,p,q,n,tmp;
            int len = record.length()+1;

            string temp = filePath;
            temp.append(tableName);
            tableName = temp;
            tableName.append(".csv");
            string relation=tableName;

            // Extract the table id from tableName
            for(i=0;i<table_index;i++)
                if( !tables[i].name.compare(tableName) )
                    break;

            j = pages[i].size() - 1;
            k = pages[i][j].id;
            tmp = strlen(pages[i][j].data);

            n = LRU.size();

            // Search for the page id in LRU
            for( p=0; p<n ;p++ )
            {
                if( LRU[p].id == k )
                {
                    if( pagesize - strlen(LRU[p].data) > len )
                    {
                        strcat( LRU[p].data , record.c_str() );
                        strcat( LRU[p].data , "+" );
                        LRU[p].modified = 1;
                        return;
                    }
                    else
                    {
                        page tmp;
                        tmp.data = new char[pagesize]();
                        strcat(tmp.data, record.c_str() );
                        strcat(tmp.data, "+");
                        cout << i << endl;
                        pages[i].push_back(tmp);
                        if( LRU.size() > pagesize )
                            LRU.pop_back();
                        LRU.push_front(tmp);
                        return;
                    }
                }
            }

            // If not found in LRU
            if( pagesize - strlen( pages[i][j].data ) > len )
            {
                strcat( pages[i][j].data , record.c_str() );
                strcat( pages[i][j].data , "+" );

                pages[i][j].modified = 1;

                if( LRU.size() > pagesize )
                    LRU.pop_back();
                LRU.push_front(pages[i][j]);
            }
            else
            {
                page tmp;
                tmp.data = new char[pagesize]();
                tmp.modified = 1;
                strcat(tmp.data, record.c_str() );
                strcat(tmp.data, "+");
                pages[i].push_back(tmp);
                if( LRU.size() > pagesize )
                    LRU.pop_back();

                LRU.push_front(tmp);
            }

            for(i=0;i<table_index;i++)
            {
                if( tables[i].name.compare(tableName)==0 )
                {
                    istringstream ss1(record);
                    string token1;
                    int ctr2=0;
                    while(getline(ss1,token1,','))
                    {
                        tables[i].unique[ctr2][token1]=1;
                        tables[i].final[tables[i].attribute[ctr2]] = tables[i].unique[ctr2].size();
                        ctr2++;
                    }
                    break;
                }
            }

            /*          for(i=0;i<pages.size();i++)
                        {
                        cout << i << endl;
                        for(int j=0;j<pages[i].size();j++)
                        {
                        cout << pages[i][j].id << endl;
                        cout << pages[i][j].data << endl;
                        }
                        }*/
        }

        ////////////////////////////////////////////////////////////////////////



        ////////////////////////////////////////////////////////////////////////
        // Function which takes a page and then writes it's contents into the 
        // file if modified bit is set
        void flush( page p )
        {
            if (p.modified != 1) return;

            int i,j,m,n;
            int x = p.id;

            n = pages.size();
            for(i=0;i<n;i++)
            {
                m = pages[i].size();
                for( j=0;j<m;j++ )
                    if( pages[i][j].id == x )
                        break;

                if( pages[i][j].id == x )
                    break;
            }

            FILE *f1;
            string name = tables[i].name;
            f1 = fopen( name.c_str(), "wb" );
            char *line = new char[pagesize]();
            int index = 0;
            for(j=0;j<m;j++)
            {
                int len = strlen( pages[i][j].data );
                for(int k=0;k<len;k++)
                {
                    if( pages[i][j].data[k] == '+' )
                        fputc( '\n' , f1 );
                    else
                        fputc( pages[i][j].data[k] , f1);
                }
            }
            fclose( f1 );

            p.modified = 0;

        }

        void flushPages()
        {
            int i,j,n;
            int u,v , k,l;
            n = LRU.size();

            for(i=0;i<n;i++)
                flush( LRU[i] );
            LRU.clear();
        }
        ////////////////////////////////////////////////////////////////////////


        void printQuery( query q )
        {
            cout << "Querytype: " << q.type << endl;
            cout << "Tablename: " << q.tables << endl;
            cout << "Columns: " << q.columns << endl;
            cout << "Distinct: " << q.distinct << endl;
            cout << "Condition: " << q.condition << endl;
            cout << "Orderby: "<< endl;
            for(int i =0; i<q.orderby.size(); i++ )
                cout << q.orderby[i] << endl;
            cout << "Groupby: " << q.groupby << endl;
            cout << "Having: " << q.having << endl;
            cout << endl;
        }


        /////////////////// Parse function for input query ///////////////////////////////////////
        query parse( string input )
        {
            query q;
            //  q.type = "select";
            join_index=0;

            vector<string> words;

            istringstream StrStream(input);
            string Token;
            while(getline(StrStream, Token, ' '))
                words.push_back(Token);
            if(words.at(0).compare("select")==0)
            {    
                q.type = "select";
                q.conditions[0] = "NA";
                q.conditions[1] = "NA";
                q.conditions[2] = "NA";
                q.conditions[2] = "NA";
                int df=0,cf=0,of=0,gf=0,hf=0,j,i;
                for(int i=0;i<words.size()-1;i++)
                {
                    j = i+1;
                    if(words.at(i).compare("from")==0)
                    {
                        while(j<words.size() && words.at(j).compare("where")!=0)
                            q.tables.append( words.at(j++) );
                    }
                    else if(words.at(i).compare("select")==0)
                    {
                        while(words.at(j).compare("from")!=0)
                            q.columns.append( words.at(j++) ); 
                    }
                    else if(words.at(i).compare("distinct")==0)
                    {
                        df=1;
                        while(words.at(j).compare("from")!=0)
                            q.distinct.append( words.at(j++) );
                    }
                    else if(words.at(i).compare("where")==0)
                    {
                        cf=1;
                        int l=0; 
                        int ndx = j+1;
                        while(j<words.size() && (words.at(j).compare("groupby")!=0&&words.at(j).compare("having")!=0&&words.at(j).compare("orderby")!=0))
                        {
                            if(words.at(j).compare("and")==0||words.at(j).compare("or")==0)
                            {
                                if(words.at(j).compare("and")==0)
                                    q.flags[l]=1;
                                if(words.at(j).compare("or")==0)
                                    q.flags[l]=0;

                                l++;
                                j++;
                            }
                            else
                                q.conditions[l].append( words.at(j++));
                        }
                        for(int xx=l;xx<3;xx++)
                            q.flags[xx]=-1;
                        for(int xx=0;xx<4;xx++)
                        {
                            if(q.conditions[xx].length()==0)
                                q.conditions[xx].append("NA");
                        }
                    }
                    else if(words.at(i).compare("on")==0)
                    {
                        int jf=1;
                        while(j<words.size()&& words.at(j).compare("join")!=0)
                        {
                            q.join_conditions[join_index].append(words.at(j++));
                        }
                        join_index++;
                    }
                    else if(words.at(i).compare("orderby")==0)
                    {
                        of=1;
                        while(j<words.size())
                            Token.append( words.at(j++) );
                    }
                    else if(words.at(i).compare("groupby")==0)
                    {
                        gf=1;
                        while(words.at(j).compare("having")!=0&&words.at(j).compare("orderby")!=0&&j<words.size())
                            q.groupby.append( words.at(j++) );
                    }
                    else if(words.at(i).compare("having")==0)
                    {
                        hf=1;
                        while(words.at(j).compare("orderby")!=0&&j<words.size())
                            q.having.append( words.at(j++) );
                    }
                }

                int hh=0;
                for(int jj=0;jj<join_index;jj++)
                {
                    int ind=0;
                    istringstream ss1(q.join_conditions[jj]);
                    string token1;
                    int new_ctr=0;
                    string newtables[2];
                    while(getline(ss1,token1,'='))
                    {
                        newtables[new_ctr++]=token1;
                    }
                    for(int ll=0;ll<2;ll++)
                    {
                        istringstream ss2(newtables[ll]);
                        string token2;
                        getline(ss2,token2,'.');
                        join_tables[hh].tab[ind]=token2;
                        getline(ss2,token2,'.');
                        join_tables[hh].att[ind++]=token2;
                    }
                    hh++;
                }

                for(int jj=0;jj<join_index;jj++)
                {
                    for(int kk=0;kk<2;kk++)
                    {
                        string togetname="./"+join_tables[jj].tab[kk]+".csv";
                        for(int ii=0;ii<table_index;ii++)
                        {
                            if(tables[ii].name.compare(togetname)==0)
                            {
                                ifstream file(tables[ii].name.c_str());
                                int ctr2=0;
                                string line;
                                while(getline(file,line,'\n'))
                                {
                                    ctr2++;
                                }
                                join_tables[jj].rows[kk]=ctr2;
                            }
                        }
                    }
                    join_tables[jj].cost=0.01*join_tables[jj].rows[0]*join_tables[jj].rows[1];
                }

                int jj,kk;
                for(jj=0;jj<join_index;jj++)
                    join_tables[jj].order=jj;
                for(jj=0;jj<join_index;jj++)
                {
                    for(kk=jj+1;kk<join_index;kk++)
                    {
                        if(join_tables[kk].cost>join_tables[jj].cost)
                        {
                            double temp=join_tables[kk].cost;
                            join_tables[kk].cost=join_tables[jj].cost;
                            join_tables[jj].cost=temp;
                            int num=kk;
                            join_tables[kk].order=jj;
                            join_tables[jj].order=num;

                        }
                    }
                }

                if( join_index > 1 )
                {
                    cout<<"Order of the given join conditions starting from the first condition:\n";
                    for(int jj=0;jj<join_index;jj++)
                    {
                        int ord=join_tables[jj].order;
                        cout<<"order "<<join_tables[jj].order<<endl;
                    }
                }
                else if( join_index == 1 )
                {
                    q.tables.clear();
                    q.tables.append( join_tables[0].tab[0] );
                    q.tables.append( "," );
                    q.tables.append( join_tables[0].tab[1] );
                    return q;
                }

                if(df==0)
                    q.distinct.append( "NA" );
                if(cf==0)
                    q.condition.append( "NA" );
                if(of==0)
                {
                    Token.clear();
                    q.orderby.clear();
                }
                if(gf==0)
                    q.groupby.append( "NA" );
                if(hf==0)
                    q.having.append( "NA" );

                words.clear();

                if( q.columns.compare("*") == 0 )
                {
                    q.columns.clear();
                    string t = filePath;
                    t.append( q.tables );
                    t.append(".csv");

                    for(i=0;i<table_index;i++)
                        if( tables[i].name.compare(t) == 0 )
                            break;

                    for(j=0;j<tables[i].num-1;j++)
                    {
                        q.columns.append( tables[i].attribute[j] );
                        q.columns.append(",");
                    }

                    q.columns.append(tables[i].attribute[j]);
                }

                for(i=0;i<Token.size();i++)
                {
                    int idx = i;
                    while( Token[idx] != ',' && idx < Token.size() )
                        idx++;
                    if( idx == Token.size() )
                    {
                        q.orderby.push_back(Token.substr(i,idx));
                        break;
                    }
                    else
                    {
                        q.orderby.push_back(Token.substr(i,idx-i));
                        i = idx;
                    }
                }
                for(i=0;i<q.orderby.size();i++)
                {
                    int bit = 0;
                    string neworderby = q.orderby[i];
                    for(j=0;j<q.orderby[i].size();j++)
                    {
                        while( j < q.orderby[i].size() && q.orderby[i][j] != '.' )
                            j++;
                        if( j == q.orderby[i].size() ) j = 0;
                        int t = j;

                        while( q.orderby[i][j] != '(' && j < q.orderby[i].size() )
                            j++;
                        if( j == q.orderby[i].size() ) break;
                        if( q.orderby[i][j] == '(' )
                        {
                            neworderby = q.orderby[i].substr(t+1,j-t-1);
                            int idx = j;
                            while(q.orderby[i][idx] != ')') idx++;
                            string tmp = q.orderby[i].substr(j+1,idx-j-1);
                            if( tmp.compare("ASC") == 0 ) bit = 0;
                            else if( tmp.compare("DESC") == 0 ) bit = 1;
                            break;
                        }
                    }
                    q.orderby[i] = neworderby;
                    q.orderasc[i] = bit;
                }
            }
            else if(words.at(0).compare("create")==0)
            {
                int j;
                q.type="create";
                for(int i=0;i<words.size()-1;i++)
                {
                    j = i+1;
                    if(words.at(i).compare("table")==0)
                    {
                        q.tables.append( words.at(j++) );

                        while(j<words.size())
                        {
                            q.attributes.append(words.at(j));
                            q.attributes.append(" ");
                            j++;

                        }
                    }
                }
                words.clear();
            }
            return q;
        }
        ///////////////////////////////// Parse function for input query ///////////////////////////////////////
        //
        //

        ///////////////////////////////////////// Join Part //////////////////////////////////////////////////////
        vvvs getrows(vvvs o,int k)
        {
            vvs d;
            int len = pages[k].size();
            for(int j=0;j<len;j++)
            {
                vs temp;
                string row(pages[k][j].data);

                int n = row.length();
                int count  = 0;
                for(int i=0;i<n;i++)
                {
                    int idx = i;
                    while( row[idx]!='+' )
                    {
                        while( row[idx] != ',' && row[idx] != '+' )
                            idx++;
                        temp.push_back( row.substr(i,idx-i) );
                        count += idx-i;
                        if( row[idx] == ',' )
                            i = ++idx;
                    }
                    i = idx;
                    d.push_back(temp);
                    if( count > pagesize )
                    {
                        o.push_back(d);
                        d.clear();
                        count = 0;
                    }
                    temp.clear();
                }
                o.push_back(d);
                d.clear();
            }

            return o;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////
        //


        ////////////////////////////////////////////////////////////////////////////////////////////////////////
        int validate( query q )
        {
            if((q.type).compare("select")==0)
            { 
                int tf = 1, cf = 1, wf = 0, gf = 0, hf = 0, of = 1;

                int w1 = 0, w2 = 0, w3 = 0, w4 = 0;

                if( q.orderby.size() == 0 ) of = 1;
                for(int i=0; i<table_index; i++)
                {
                    for(int j=0; j<tables[i].num; j++)
                    {
                        if( q.conditions[0].find( tables[i].attribute[j] ) != string::npos || !q.conditions[0].compare("NA") ) w1 = 1;
                        if( q.conditions[1].find( tables[i].attribute[j] ) != string::npos || !q.conditions[1].compare("NA") ) w2 = 1;
                        if( q.conditions[2].find( tables[i].attribute[j] ) != string::npos || !q.conditions[2].compare("NA") ) w3 = 1;
                        if( q.conditions[3].find( tables[i].attribute[j] ) != string::npos || !q.conditions[3].compare("NA") ) w4 = 1;

                        if( q.groupby.find( tables[i].attribute[j] ) != string::npos || !q.groupby.compare("NA") ) gf = 1;
                        if( q.having.find( tables[i].attribute[j] ) != string::npos || !q.groupby.compare("NA") ) hf = 1;

                        for(int k=0;k<q.orderby.size();k++)
                            if( q.orderby[k].find( tables[i].attribute[j] ) != string::npos ) 
                                of = 1&&of;
                    }
                }
                if( q.conditions[0].compare("NA") == 0 ) wf = 1;
                else
                    wf = w1 && w2 && w3 && w4;

                for(int k=0;k<q.orderby.size();k++)
                {
                    int bit = 0;
                    for(int i=0;i<table_index;i++)
                        for(int j=0;j<tables[i].num; j++)
                            if( q.orderby[k].find( tables[i].attribute[j] ) != string::npos || !q.orderby[k].compare("NA") )
                                bit = 1;
                    of = of && bit;
                }

                istringstream s1(q.tables);
                string token;
                while( getline( s1, token, ',' ) )
                {
                    int bit = 0;

                    string tmp = filePath;
                    token.append(".csv");
                    tmp.append(token);
                    token = tmp;

                    for(int i=0;i<table_index;i++)
                        if( tables[i].name == token )
                            bit = 1;
                    tf = tf && bit;
                }

                if( q.columns.compare("*") != 0 )
                {
                    istringstream s2(q.columns);
                    while( getline( s2, token, ',' ) )
                    {
                        int bit = 0;
                        for(int i=0;i<table_index;i++)
                            for(int j=0; j<tables[i].num; j++)
                            {
                                string s = string(tables[i].attribute[j]);
                                if( token.find( s ) != string::npos )
                                    bit = 1;
                            }
                        cf = cf && bit;
                    }
                }

                if( join_index == 1 )
                {
                    int i,j;
                    int t1 = 0, t2 = 0;
                    string t = filePath;
                    t.append( join_tables[0].tab[0] );
                    t.append(".csv");

                    tf = 0;
                    for(i=0;i<table_index;i++)
                        if( tables[i].name.compare(t) == 0 )
                        {
                            for(j=0;j<tables[i].num;j++)
                                if( tables[i].attribute[j].compare( join_tables[0].att[0] ) == 0 )
                                    t1 = 1;
                        }
                    
                    t = filePath;
                    t.append( join_tables[0].tab[1] );
                    t.append(".csv");
                    for(i=0;i<table_index;i++)
                        if( tables[i].name.compare(t) == 0 )
                        {
                            for(j=0;j<tables[i].num;j++)
                                if( tables[i].attribute[j].compare( join_tables[0].att[1] ) == 0 )
                                    t2 = 1;
                        }

                    tf = t1 && t2;
                    gf = hf = of = 1;
                }

            //    cout << tf << " " << cf << " " << wf << " " << gf << " " << " " << hf << " " << of << endl;
                return ( tf && cf && wf && gf && hf && of );
                return 1;
            }
            if((q.type).compare("create")==0)
            {
                int bit = 1;
                string tmp=filePath;
                tmp.append( q.tables );
                tmp.append(".csv");
                for(int i=0;i<table_index;i++)
                    if( tables[i].name == tmp )
                        bit = 0;

                return bit; 

            }
        }
        /////////////////////////////////////////////////////////////////////////////////////////////////


        bool isEqual_CaseInsensitive(const string &a, const string &b)
        {
            if( a.length() != b.length() ) return false;
            for(int i=0;i<a.length();i++)
            {
                if(toupper(a[i]) != toupper(b[i]))
                    return false;

            }
            return true;
        }


        //////////////////////////// Tests each row on every where condition /////////////////////
        int reduceData( query q, vs row, int index)
        {
            int code;
            string table;
            string col;
            string op;
            string value;

            int i,j,n,k;

            if( q.conditions[0].compare("NA") == 0 ) return 1;

            int bit = 0,prev_bit;
            for( int u = 0; u < 4; u++ )
            {
                if( q.conditions[u].compare("NA") == 0 ) continue;

                bit = 0;
                string clause = q.conditions[u];
                n = clause.length();
                for(i=0;i<n;i++)
                {
                    while( clause[i] != '.' && i < n ) i++;
                    if( i == n ) i = 0;
                    int tmp = i;

                    i++;
                    int idx = i;
                    while( idx<n && clause[idx] != '=' && clause[idx] != '<' && clause[idx] != '>' && clause[idx] != '!' )
                        idx++;
                    col = clause.substr(i,idx-i);
                    i = idx;

                    if( clause[i] == '=' ) code = 1;
                    else if( clause[i] == '<' && clause[i+1] == '=' ){ code = 2; i++;}
                    else if( clause[i] == '>' && clause[i+1] == '=' ){ code = 3; i++;}
                    else if( clause[i] == '!' && clause[i+1] == '=' ){ code = 4; i++;}
                    else if( clause[i] == '<' ) code = 5;
                    else if( clause[i] == '>' ) code = 6;
                    else
                    {
                        size_t z = clause.find("LIKE");
                        if( z != std::string::npos )
                        {
                            code = 7;
                            i = z+3;
                        }
                        col = clause.substr(tmp+1,z-tmp-1);
                    }
                    break;
                }
                i++;
                value = clause.substr(i,n);

                for(i=0;i<tables[index].num;i++)
                    if( tables[index].attribute[i].compare( col ) == 0 ) break;

                if( i == tables[index].num ) return -1;

                int colno = i;

                if( code == 1 )
                {
                    if( !row[colno].compare(value) )
                        bit = 1;
                }
                else if( code == 2 )
                {
                    if( atof(row[colno].c_str()) <= atof(value.c_str()) )
                        bit = 1;
                }
                else if( code == 3 )
                {
                    if( atof(row[colno].c_str()) >= atof(value.c_str()) )
                        bit = 1;
                }
                else if( code == 4 )
                {
                    if( atof(row[colno].c_str()) != atof(value.c_str()) )
                        bit = 1;
                }
                else if( code == 5 )
                {
                    if( atof(row[colno].c_str()) < atof(value.c_str()) )
                        bit = 1;
                }
                else if(code==7)
                {
                    if(isEqual_CaseInsensitive(row[colno].c_str(),value.c_str()))
                        bit=1;
                }

                else if(code==6)
                {
                    if( atof(row[colno].c_str()) > atof(value.c_str()) )
                        bit = 1;
                }

                if( u > 0 )
                {
                    if( q.flags[u-1] == 1 ) bit = prev_bit && bit;
                    else if( q.flags[u-1] == 0 ) bit = prev_bit || bit;
                }
                prev_bit = bit;
            }

            return bit;
        }
        ///////////////////////////////////////////////////////////////////////////////////////


        ///////////////// To compare any two rows while merging ///////////////////////////////
        bool comparator(vs a,vs b,int idx[], int bit[],int type[],int n)
        {
            int i=0,equal=1;
            while( equal && i<n )
            {
                if( type[i] == 1 )
                {
                    if( a[idx[i]].compare(b[idx[i]]) != 0 ){
                        equal = 0;
                        break;
                    }
                }
                else
                {
                    if ( ( atof(a[idx[i]].c_str())-atof(b[idx[i]].c_str())) != 0)
                    {
                        equal = 0; 
                        break;
                    }
                }
                i++;
            }

            if( i == n ) i = n-1;

            if(bit[i]==1)
            {
                if(type[i]==1)
                    return (a[idx[i]].compare(b[idx[i]])>=0)?true:false;
                else
                    return ((atof(a[idx[i]].c_str())-atof(b[idx[i]].c_str()))>=0)?true:false;
            }
            else
            {
                if(type[i]==1)
                    return (a[idx[i]].compare(b[idx[i]])>=0)?false:true;
                else
                    return ((atof(a[idx[i]].c_str())-atof(b[idx[i]].c_str()))>=0)?false:true;
            }
        }
        /////////////////////////////////////////////////////////////////////////////////////////


        /////////////////////////////////////////////////////////////////////////////////////////
        vvs merge(vvs a, vvs b, int idx[], int bit[], int type[], int n1 )
        {

            int i,j;
            int n = a.size(), m = b.size();
            vvs result;

            if( m == 0 ) return a;
            else if( n == 0 ) return b;

            for(i=0,j=0; i<n||j<m ; )
            {
                if( i < n && j < m )
                {
                    if( comparator(b[j],a[i],idx,bit,type,n1) )
                    {
                        result.push_back(b[j++]);
                    }
                    else
                    {
                        result.push_back(a[i++]);
                    }
                }
                else if( i < n )
                {
                    result.push_back(a[i++]);
                }
                else if( j < m )
                {
                    result.push_back(b[j++]);
                }
            }

            return result;
        }
        //////////////////////////////////////////////////////////////////////////////////

        vvvs TwoPhaseMerge(vvvs output,int idx[], int q_orderasc[], int type[], int q_size )
        {
            int i,j,n,k,len;
            for(i=0;i<output.size();i++)
                sort(output[i].begin(), output[i].end(), comparefun(idx,q_orderasc,type,q_size) );

            n = output.size();
            vvs mrg;
            // Two Phase Merge

            for(int it=2; it/2 < n; it = it*2 )
            {
                vvs a1, a2;
                for(i = 0; i < output.size(); i+=it )
                {
                    for(j=i;j<i+it/2&&j<output.size();j++)
                    {
                        for(int tmp=0;tmp<output[j].size();tmp++)
                            a1.push_back(output[j][tmp]);
                        output[j].clear();
                    }
                    for(;j<i+it&&j<output.size();j++)
                    {
                        for(int tmp=0;tmp<output[j].size();tmp++)
                            a2.push_back(output[j][tmp]);
                        output[j].clear();
                    }

                    mrg = merge(a1,a2,idx,q_orderasc,type,q_size);

                    int index = i;
                    int count = 0;
                    for(j=0;j<mrg.size();j++)
                    {
                        for(int y=0;y<mrg[j].size();y++)
                        {
                            count += mrg[j][y].length()+1;
                        }

                        output[index].push_back(mrg[j]);
                        if( count > pagesize )
                        {
                            index++;
                            count = 0;
                        }
                    }
                    a1.clear();
                    a2.clear();
                    mrg.clear();
                }

            }
            return output;

        }

        ////////////////////////// Function to execute Select Query //////////////////////
        void executeSelectQuery( query q )
        {
            vvvs answer;
            vvs data;
            int i,j,n,k;
            string tmp = filePath;
            tmp.append(q.tables);
            tmp.append(".csv");

            for(k=0;k<table_index;k++)
                if( !tables[k].name.compare(tmp) )
                    break;

            int len = pages[k].size();
            for(j=0;j<len;j++)
            {
                vector<string> temp;
                string row(pages[k][j].data);

                n = row.length();
                int count = 0;
                for(i=0;i<n;i++)
                {
                    int idx = i;
                    while(row[idx]!='+') 
                    {
                        while( row[idx] != ',' && row[idx] != '+' )
                            idx++;
                        temp.push_back( row.substr(i,idx-i) );
                        count += idx-i;
                        if( row[idx] == ',' )
                            i = ++idx;
                    }
                    i = idx;
                    data.push_back(temp);
                    if( count > pagesize )
                    {
                        answer.push_back( data );
                        data.clear();
                        count = 0;
                    }
                    temp.clear();
                }
                answer.push_back(data);
                data.clear();
            }

            n = answer.size();
            int count = 0;
            vvvs output;
            for(i=0;i<answer.size();i++)
            {
                int m = answer[i].size();
                for(j=0;j<m;j++)
                {
                    int bit = reduceData( q, answer[i][j], k );
                    if( bit == 1 )
                    {
                        for(int w = 0;w < answer[i][j].size();w++)
                            count += answer[i][j][w].length();
                        data.push_back(answer[i][j]);

                        if( count > pagesize )
                        {
                            output.push_back(data);
                            data.clear();
                            count = 0;
                        }
                    }
                }
            }
            output.push_back(data);
            data.clear();
            answer.clear();

            if( q.orderby.size() > 0 )
            {
                /// Orderby Clause Begins
                int idx[100];
                for(i=0;i<q.orderby.size();i++)
                {
                    tmp = q.orderby[i];
                    for(j=0;j<tables[k].num;j++)
                        if( tables[k].attribute[j].compare(tmp) == 0 ) break;
                    idx[i] = j;
                }

                int type[100];
                for(i=0;i<q.orderby.size();i++)
                {
                    if( tables[k].records[idx[i]] == 2 || tables[k].records[idx[i]] == 3 )
                        type[i] = 0;
                    else
                        type[i] = 1;
                }

                /// Final merge
                output = TwoPhaseMerge(output,idx,q.orderasc,type,q.orderby.size());
            }

            map<int,int> mp;
            for(i=0;i<q.columns.size();i++)
            {
                int idx = i;
                while( idx < q.columns.size() && q.columns[idx] != ',' )
                    idx++;
                if( idx == q.columns.size() )
                {
                    string tmp = q.columns.substr(i,idx-i);
                    for(j=0;j<table_index;j++)
                    {
                        for(int p=0;p<tables[j].num;p++)
                            if( !tables[j].attribute[p].compare(tmp) )
                                mp[p] = 1;
                    }
                }
                else
                {
                    string tmp = q.columns.substr(i,idx-i);
                    for(j=0;j<table_index;j++)
                    {
                        for(int p=0;p<tables[j].num;p++)
                            if( !tables[j].attribute[p].compare(tmp) )
                                mp[p] = 1;
                    }
                    i = idx;
                }

            }

            for(i=0;i<output.size();i++)
            {
                cout << "\n";
                for(j=0;j<output[i].size();j++)
                {
                    for(int z=0;z<output[i][j].size();z++)
                        if( mp.count(z) )
                            cout << output[i][j][z] << " ";
                    cout << endl;
                }
            }
        }
        /////////////////////////////////////////////////////////////////////////////

        void joinfun(query q)
        {
            vvvs o1,o2;
            int i,j,k,k1,k2,n,m;
            int l1,l2;

            string tmp = filePath;
            tmp.append( join_tables[0].tab[0] );
            tmp.append( ".csv" );

            for(k1=0;k1<table_index;k1++)
                if( !tables[k1].name.compare(tmp) )
                    break;
            o1 = getrows(o1,k1);

            tmp = filePath;
            tmp.append( join_tables[0].tab[1] );
            tmp.append(".csv");

            for(k2=0;k2<table_index;k2++)
                if( !tables[k2].name.compare(tmp) )
                    break;
            o2 = getrows(o2,k2);

            l1 = tables[k1].num;
            l2 = tables[k2].num;

            int idx1[1],idx2[1], q_orderasc1[1], q_orderasc2[1], type1[1], type2[1];

            for(i=0;i<tables[k1].num;i++)
            {
                if( tables[k1].attribute[i].compare( join_tables[0].att[0] ) == 0 )
                {
                    if( tables[k1].records[i] == 2 || tables[k1].records[i] == 3 )
                        type1[0] = 0;
                    else
                        type1[0] = 1;
                    idx1[0] = i;
                }
            }
            
            for(i=0;i<tables[k2].num;i++)
            {
                if( tables[k2].attribute[i].compare( join_tables[0].att[1] ) == 0 )
                {
                    if( tables[k2].records[i] == 2 || tables[k2].records[i] == 3 )
                        type2[0] = 0;
                    else
                        type2[0] = 1;
                    idx2[0] = i;
                }
            }
            
            q_orderasc1[0] = 0;
            q_orderasc2[0] = 0;

            o1 = TwoPhaseMerge(o1,idx1,q_orderasc1,type1,1);
            o2 = TwoPhaseMerge(o2,idx2,q_orderasc2,type2,1);

            vvvs output;
            vvs ans;
            vs tp;
            int i1,i2,i3,j1,j2,j3;

            for(i1=0;i1<o1.size();i1++)
            {
                for(i2=0;i2<o1[i1].size();i2++)
                {
                    for(j1=0;j1<o2.size();j1++)
                    {
                        for(j2=0;j2<o2[j1].size();j2++)
                        {
                            if( type1[0] == 0 || type1[0] == 3 )
                            {
                                if( atof( o1[i1][i2][idx1[0]].c_str() ) == atof( o2[j1][j2][idx2[0]].c_str() ) )
                                {
                                    for(i3=0;i3<o1[i1][i2].size();i3++)
                                        tp.push_back(o1[i1][i2][i3]);
                                    for(j3=0;j3<o2[j1][j2].size();j3++)
                                        tp.push_back(o2[j1][j2][j3]);
                                    ans.push_back(tp);
                                    tp.clear();
                                }
                            }
                            else
                            {
                                if( o1[i1][i2][idx1[0]].compare( o2[j1][j2][idx2[0]] ) == 0 )
                                {
                                    for(i3=0;i3<o1[i1][i2].size();i3++)
                                        tp.push_back(o1[i1][i2][i3]);
                                    for(j3=0;j3<o1[j1][j2].size();j3++)
                                        tp.push_back(o2[j1][j2][j3]);
                                    ans.push_back(tp);
                                    tp.clear();
                                }
                            }

                        }
                    }
                }
            }
            
            map<int,int> mp;
            for(i=0;i<q.columns.size();i++)
            {
                int idx = i;
                while( idx < q.columns.size() && q.columns[idx] != ',' )
                    idx++;
                if( idx == q.columns.size() )
                {
                    string tmp = q.columns.substr(i,idx-i);
                    for(j=0;j<table_index;j++)
                    {
                        for(int p=0;p<tables[j].num;p++)
                            if( !tables[j].attribute[p].compare(tmp) )
                                mp[10*j+p] = 1;
                    }
                }
                else
                {
                    string tmp = q.columns.substr(i,idx-i);
                    for(j=0;j<table_index;j++)
                    {
                        for(int p=0;p<tables[j].num;p++)
                            if( !tables[j].attribute[p].compare(tmp) )
                                mp[10*j+p] = 1;
                    }
                    i = idx;
                }
            }

            for(i=0;i<ans.size();i++)
            {
                for(j=0;j<ans[i].size();j++)
                {
                    if( j < l1 && mp.count(10*k1+j) )
                        cout << ans[i][j] << " ";
                    else if( j > l1 && mp.count(10*k2+(j-l1)) )
                        cout << ans[i][j] << " ";
                }
                cout << endl;
            }
        }

        void selectCommand(string input)
        {
            query q = parse( input );
            int bit = validate( q );

            if( bit )
            {
                printQuery(q);
                if( join_index == 1 ) 
                    joinfun(q);
                else
                    executeSelectQuery(q);
            }
            else
                cout << "Invalid Query\n";
        }
        void createCommand(string input)
        {
            query q=parse(input);

            int bit=validate(q);
            if(bit)
            {

                string tmp=q.tables;
                string x=tmp.c_str();
                x.append(".csv");

                FILE *f;
                f=fopen(x.c_str(),"w");
                fclose(f);

                string data=tmp.append(".data");
                FILE *fd=fopen(data.c_str(),"w");
                q.attributes.erase(0,1);
                q.attributes.erase(q.attributes.size()-2,1);
                fputs(q.attributes.c_str(),fd);
                fclose(fd);

                char buff[100];
                FILE *np=fopen(data.c_str(),"r+");
                fclose(np);


                printQuery(q);

                FILE *config=fopen("config.txt","a+");
                fputs("BEGIN\n",config);
                fputs(q.tables.c_str(),config);
                fputc( '\n', config );

                string token;
                istringstream ss( q.attributes );
                while( getline( ss, token, ',' ) )
                {
                    fputs( token.c_str(), config);
                    fputc( '\n', config );
                }

                fputs("END\n",config);
                fclose(config);
            }
            else
                cout<<"Invalid Query\n";
        }

        void queryType(string input)
        {
            char p[100];
            string Token;
            istringstream StrStream(input);

            getline(StrStream, Token, ' ');
            if(Token.compare("select")==0)
            {
                selectCommand(input);
            }
            else if(Token.compare("create")==0)
                createCommand(input);

        }


        int V(string tableName, string attributeName)
        {
            string tmp = filePath;
            tmp.append(tableName);
            tmp.append(".csv");
            tableName = tmp;

            int count;
            for(int i=0;i<table_index;i++)
            {
                if(tables[i].name.compare(tableName)==0)
                {
                    int j = 0;
                    for(j=0;j<tables[i].num;j++)
                        if( tables[i].attribute[j].compare(attributeName) == 0 )
                            break;
                    count=tables[i].final[attributeName];
                }
            }
            return count;

        }


};

int main(int argc,char *argv[])
{
    DBSystem file1;
    file1.readConfig(argv[1]);
    file1.populateDBInfo();

    int t;
    char ch;
    cout<<"Number of queries: ";
    cin>>t;
    scanf("%c",&ch);
       
    while(t--)
    {
       cout<<"Enter query: ";
       char query[200];
       scanf("%[^\n]",query);
       scanf("%c",&ch); 
       file1.queryType(query);
    }
       
//    file1.queryType("select s_id,s_order,s_num from table2 where table2.s_num<230 and table2.s_num>205 or table2.s_idLIKE48 orderby table2.s_num(DESC) ");
//    file1.queryType("select s_num,e_id from table1 join table2 on table1.e_num=table2.s_num");
//    file1.queryType("select * from table1 where table1.e_id>147 orderby table1.e_id(DESC) ");
    return 0;
}

