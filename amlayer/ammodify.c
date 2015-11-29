# include <stdio.h>
# include "pf.h"
# include "am.h"



AM_ModifyEntry(fileDesc,attrType,attrLength,value,recId,newRecId)
int fileDesc; /* file Descriptor */
char attrType; /* 'c' , 'i' or 'f' */
int attrLength; /* 4 for 'i' or 'f' , 1-255 for 'c' */
char *value;/* Value of key whose corr recId is to be deleted */
int recId; /* id of the record to delete */
int newRecId; /* id of the record to replace the old record */

{
	char *pageBuf;/* buffer to hold the page */
	int pageNum; /* page Number of the page in buffer */
	int index;/* index where key is present */
	int status; /* whether key is in tree or not */
	short nextRec;/* contains the next record on the list */
	short oldhead; /* contains the old head of the list */
	short temp; 
	char *currRecPtr;/* pointer to the current record in the list */
	AM_LEAFHEADER head,*header;/* header of the page */
	int recSize; /* length of key,ptr pair for a leaf */
	int tempRec; /* holds the recId of the current record */
	int errVal; /* holds the return value of functions called within 
				                            this function */
	int i; /* loop index */


	/* check the parameters */
	if ((attrType != 'c') && (attrType != 'f') && (attrType != 'i'))
		{
		 AM_Errno = AME_INVALIDATTRTYPE;
		 return(AME_INVALIDATTRTYPE);
                }

	if (value == NULL) 
		{
		 AM_Errno = AME_INVALIDVALUE;
		 return(AME_INVALIDVALUE);
                }

	if (fileDesc < 0) 
		{
		 AM_Errno = AME_FD;
		 return(AME_FD);
                }

	/* initialise the header */
	header = &head;
	
	/* find the pagenumber and the index of the key to be deleted if it is
	there */
	status = AM_Search(fileDesc,attrType,attrLength,value,&pageNum,
			   &pageBuf,&index);
	
	/* check if return value is an error */
	if (status < 0) 
		{
		 AM_Errno = status;
		 return(status);
                }
	
	/* The key is not in the tree */
	if (status == AM_NOT_FOUND) 
		{
		 AM_Errno = AME_NOTFOUND;
		 return(AME_NOTFOUND);
                }
	
	bcopy(pageBuf,header,AM_sl);
	recSize = attrLength + AM_ss;
	currRecPtr = pageBuf + AM_sl + (index - 1)*recSize + attrLength;
	bcopy(currRecPtr,&nextRec,AM_ss);
	
	/* search the list for recId */
	while(nextRec != 0)
	{
		bcopy(pageBuf + nextRec,&tempRec,AM_si);
		
		/* found the recId to be deleted */
		if (recId == tempRec)
		{
			/* Delete recId */
			bcopy(pageBuf + nextRec + AM_si,currRecPtr,AM_ss);
			header->numinfreeList++;
			oldhead = header->freeListPtr;
			header->freeListPtr = nextRec;
			bcopy(&oldhead,pageBuf + nextRec + AM_si,AM_ss);
			AM_InsertToLeafFound(pageBuf,newRecId,index,header);
			break;
		}
		else 
	        {
			/* go over to the next item on the list */
			currRecPtr = pageBuf + nextRec + AM_si;
			bcopy(currRecPtr,&nextRec,AM_ss);
		}
	}
	
	/* if end of list reached then key not in tree */
	if (nextRec == AM_NULL)
		{
		 AM_Errno = AME_NOTFOUND;
		 return(AME_NOTFOUND);
                }
	
	/* copy the header onto the buffer */
	bcopy(header,pageBuf,AM_sl);
	
	errVal = PF_UnfixPage(fileDesc,pageNum,TRUE);
	
	/* empty the stack so that it is set for next amlayer call */
	AM_EmptyStack();
	  {
	   AM_Errno = AME_OK;
	   return(AME_OK);
          }
}
