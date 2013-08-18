drop table Trans;
create table Trans (
	gender VARCHAR2(10),
	cars NUMBER,
	travCost VARCHAR2(10),
	income VARCHAR2(10),
	transMode VARCHAR2(10) );
insert into Trans (gender, cars, travCost, income, transMode) values ('Male', 0, 'Cheap', 'Low', 'Bus');
insert into Trans (gender, cars, travCost, income, transMode) values ('Male', 1, 'Cheap', 'Medium', 'Bus');
insert into Trans (gender, cars, travCost, income, transMode) values ('Female', 1, 'Cheap', 'Medium', 'Train');
insert into Trans (gender, cars, travCost, income, transMode) values ('Female', 0, 'Cheap', 'Low', 'Bus');
insert into Trans (gender, cars, travCost, income, transMode) values ('Male', 1, 'Cheap', 'Medium', 'Bus');
insert into Trans (gender, cars, travCost, income, transMode) values ('Male', 0, 'Standard', 'Medium', 'Train');
insert into Trans (gender, cars, travCost, income, transMode) values ('Female', 1, 'Standard', 'Medium', 'Train');
insert into Trans (gender, cars, travCost, income, transMode) values ('Female', 1, 'Expensive', 'High', 'Car');
insert into Trans (gender, cars, travCost, income, transMode) values ('Male', 2, 'Expensive', 'Medium', 'Car');
insert into Trans (gender, cars, travCost, income, transMode) values ('Female', 2, 'Expensive', 'High', 'Car');

-- ===============================================================================================================

-- Recursive DT generation Algorithm
-- Result is represented by a set of rules(strings)
-- intermediate views are created to facilitate the recursive procedure 
CREATE OR REPLACE PROCEDURE CreateDT (
	table_name VARCHAR2, -- name of the training table
	class_name VARCHAR2 -- name of the class column
) AUTHID CURRENT_USER AS 
-- Type Declarations
TYPE RuleList IS TABLE OF VARCHAR2(200);
TYPE NumberList IS TABLE OF INTEGER;
TYPE RefCursor IS REF CURSOR;

TYPE TableNode IS RECORD (
	viewName VARCHAR2(200), -- name corresponding view
	rule VARCHAR2(200), -- content of corresponding rule
	entropy REAL, -- value of entropy
	giniIndex REAL, -- value of gini index
	classificationErr REAL, -- value of classification error
	rowTotal NUMBER -- total number of rows 
);
TYPE TableNodeList IS TABLE OF TableNode;

TYPE CandidateAttr IS RECORD (
	attrName VARCHAR2(32), -- attribute name
	infoGain_Ent REAL, -- information gain of Entropy for this candidate attribure
	infoGain_Gini REAL, -- information gain of Gini Index for this candidate attribure
	infoGain_ClsErr REAL, -- information gain of Classification error for this candidate attribure
	tableNodes TableNodeList := TableNodeList() -- corresponding table nodes if split by this attribute
);
TYPE CandidateAttrList IS TABLE OF CandidateAttr; -- table of candidate attributes

-- variable declarations
resultRules            RuleList := RuleList(); -- The final result will be stored in the list
rootNode               TableNode; -- root node to start
rootView               VARCHAR2(32) := 'TransView'; -- root view name
vi                     PLS_INTEGER;





-- Entropy calculation sub-program
PROCEDURE CalcEntropy(
	view_name VARCHAR2, -- view (table/sub-table) to calculate on
    entropy OUT REAL, 	-- entropy value
    rowTotal OUT PLS_INTEGER)   -- total row counts
IS
	rowCountList NumberList := NumberList(); -- list of row counts
	curRowCount INTEGER; -- current row count
	rowCountCursor RefCursor;	-- cursor for current row count
BEGIN
	DBMS_OUTPUT.PUT_LINE('calc entropy for: ' || view_name);
	-- Init.
	entropy := 0;
	rowTotal := 0;

	-- get number of each class in the table
	OPEN rowCountCursor FOR 
	'SELECT COUNT(*) FROM ' || view_name || ' GROUP BY ' || class_name; 
	LOOP
	FETCH rowCountCursor INTO curRowCount;
	EXIT WHEN rowCountCursor%NOTFOUND;
	rowCountList.EXTEND;
	rowCountList(rowCountList.COUNT) := curRowCount;
	rowTotal := rowTotal + curRowCount;
	END LOOP;
	CLOSE rowCountCursor;

	-- Calculate entropy
	FOR i IN 1..rowCountList.COUNT 
	LOOP
	entropy := entropy - (rowCountList(i) / rowTotal) * LOG(2, (rowCountList(i) / rowTotal));
	END LOOP;
	entropy := ROUND(entropy, 3); -- Precision of 3 
	rowCountList.TRIM(rowCountList.COUNT); -- Clear
END CalcEntropy;




--Calculate Gini Index
PROCEDURE CalcGiniIndex(
	view_name VARCHAR2, -- view (table/sub-table) to calculate on
    giniindex OUT REAL, 	-- gini index value
    rowTotal OUT PLS_INTEGER)   -- total row counts
IS
	rowCountList NumberList := NumberList(); -- list of row counts
	curRowCount INTEGER; -- current row count
	rowCountCursor RefCursor;	-- cursor for current row count
BEGIN
	DBMS_OUTPUT.PUT_LINE('calc Gini Index for: ' || view_name);
	-- Init.
	giniindex := 1;
	rowTotal := 0;

	-- get number of each class in the table
	OPEN rowCountCursor FOR 
	'SELECT COUNT(*) FROM ' || view_name || ' GROUP BY ' || class_name; 
	LOOP
	FETCH rowCountCursor INTO curRowCount;
	EXIT WHEN rowCountCursor%NOTFOUND;
	rowCountList.EXTEND;
	rowCountList(rowCountList.COUNT) := curRowCount;
	rowTotal := rowTotal + curRowCount;
	END LOOP;
	CLOSE rowCountCursor;

	-- Calculate Gini Index
	FOR i IN 1..rowCountList.COUNT 
	LOOP
	giniindex := giniindex - (rowCountList(i) / rowTotal) *  (rowCountList(i) / rowTotal);
	END LOOP;
	giniindex := ROUND(giniindex, 3); -- Precision of 3 
	rowCountList.TRIM(rowCountList.COUNT); -- Clear
END CalcGiniIndex;




--Calculate classification Error
PROCEDURE CalcClassificationError(
	view_name VARCHAR2, -- view (table/sub-table) to calculate on
    classificationError OUT REAL, 	-- classification error value
    rowTotal OUT PLS_INTEGER)   -- total row counts
IS
	rowCountList NumberList := NumberList(); -- list of row counts
	curRowCount INTEGER; -- current row count
	maxP        REAL;
	rowCountCursor RefCursor;	-- cursor for current row count
BEGIN
	DBMS_OUTPUT.PUT_LINE('calc classification error for: ' || view_name);
	-- Init.
	classificationError := 1;
	rowTotal := 0;
	maxP := 0;

	-- get number of each class in the table
	OPEN rowCountCursor FOR 
	'SELECT COUNT(*) FROM ' || view_name || ' GROUP BY ' || class_name; 
	LOOP
	FETCH rowCountCursor INTO curRowCount;
	EXIT WHEN rowCountCursor%NOTFOUND;
	rowCountList.EXTEND;
	rowCountList(rowCountList.COUNT) := curRowCount;
	rowTotal := rowTotal + curRowCount;
	END LOOP;
	CLOSE rowCountCursor;

	-- Calculate Classification Error
	FOR i IN 1..rowCountList.COUNT 
	LOOP
	IF rowCountList(i) / rowTotal > maxP  THEN
	maxP := rowCountList(i) / rowTotal;
	--DBMS_OUTPUT.PUT_LINE(maxP);
	END IF;
	END LOOP;
	
	classificationError := classificationError - maxP;
	classificationError := ROUND(classificationError, 3); -- Precision of 3 
	DBMS_OUTPUT.PUT_LINE(classificationError);
	rowCountList.TRIM(rowCountList.COUNT); -- Clear
END CalcClassificationError;





-- Decision Tree generation sub-program, will be called recursively
PROCEDURE GenerateDT (
	curRootNode TableNode)
IS
	attrNames RefCursor; -- cursor of attrbute names
	curAttrName VARCHAR2(32); -- current attribute name
	attrValues RefCursor; -- cursor of attribute values
	curAttrValue VARCHAR2(10); -- current attribute value
	newTableNode TableNode; -- new table node 
	viewCreateStmt VARCHAR2(400); -- statement to create the view for new node
	subAttrNames RefCursor; -- cursor of sub attrbute names
	curSubAttrName VARCHAR2(32); -- current sub attribute name
	infoGain_Ent REAL; -- information gain of entropy
	infoGain_Gini REAL; -- information gain of gini index
	infoGain_ClsErr REAL; -- information gain of classification error
	newNodeList TableNodeList := TableNodeList(); -- list of new table nodes
	curCandidateAttr CandidateAttr; -- current candidate attribute
	candidateAttrs CandidateAttrList := CandidateAttrList(); -- candidate attributes with their resulting sub-tables
	maxInfoGain_Ent REAL := 0; -- max information gain of Entropy
	maxInfoGain_Gini REAL := 0; -- max Information gain of Gini Index
	maxInfoGain_ClsErr REAL := 0; -- max Information gain of Classification Error	
	chosenAttr CandidateAttr; -- attribute being chosen for split
	theClassNameCur RefCursor; -- cursor of class name when entropy = 0
	theClassName VARCHAR2(10); -- class name when entropy = 0
BEGIN
	-- base case of recursion, if the entropy is 0, a leaf has been found and rule should be added to final result 
	IF curRootNode.entropy = 0 AND curRootNode.giniIndex = 0 AND curRootNode.classificationErr = 0 THEN
		OPEN theClassNameCur FOR 
		'SELECT ' || class_name || ' FROM ' || curRootNode.viewName;
		LOOP -- should be only one record
			FETCH theClassNameCur INTO theClassName;
			EXIT WHEN theClassNameCur%NOTFOUND;
		END LOOP;
		resultRules.EXTEND;
		resultRules(resultRules.COUNT) := curRootNode.rule || ' then class = ' || theClassName;
		RETURN;
	END IF;
	OPEN attrNames FOR 
    'SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER(''' || curRootNode.viewName || ''') AND COLUMN_NAME <> UPPER(''' || class_name || ''') ORDER BY COLUMN_NAME';
	LOOP -- for each attribute column
		FETCH attrNames INTO curAttrName;
		EXIT WHEN attrNames%NOTFOUND;
		DBMS_OUTPUT.PUT_LINE('attr name: ' || curAttrName);
		-- init information gain for this attribute
		infoGain_Ent := curRootNode.entropy;
		infoGain_Gini := curRootNode.giniIndex;
		infoGain_ClsErr := curRootNode.classificationErr;
		
		OPEN attrValues FOR 
		'SELECT DISTINCT '|| curAttrName || ' FROM ' || curRootNode.viewName || ' ORDER BY ' || curAttrName;
		LOOP -- for each distinct value of current attribute
			vi := vi + 1;
			FETCH attrValues INTO curAttrValue;
			EXIT WHEN attrValues%NOTFOUND;
			DBMS_OUTPUT.PUT_LINE('  attr value: ' || curAttrValue);
			-- generate a new table node
			newTableNode.viewName := curAttrName || '_' || vi;
			-- add rules incrementally
			IF LENGTH(curRootNode.rule) > 0 THEN
				newTableNode.rule := curRootNode.rule || ' and ' || curAttrName || ' = ' || curAttrValue;
			ELSE
				newTableNode.rule := curAttrName || ' = ' || curAttrValue;
			END IF;
			-- generate view for this table node, which will be used in the recursive call
			viewCreateStmt := 'CREATE OR REPLACE VIEW ' || newTableNode.viewName || ' AS SELECT ';
			OPEN subAttrNames FOR 
			'SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER(''' || curRootNode.viewName || ''') AND COLUMN_NAME <> UPPER(''' || curAttrName || ''')';
			LOOP -- for each attributes except the current one
				FETCH subAttrNames INTO curSubAttrName;
				EXIT WHEN subAttrNames%NOTFOUND;
				IF subAttrNames%ROWCOUNT > 1 THEN
					viewCreateStmt := viewCreateStmt || ',';
				END IF;
				viewCreateStmt := viewCreateStmt || curSubAttrName;			
			END LOOP; -- end loop each attributes except the current one
			CLOSE subAttrNames;
			viewCreateStmt := viewCreateStmt || ' FROM ' || curRootNode.viewName || ' WHERE ' || curAttrName || '=''' || curAttrValue || '''';
			EXECUTE IMMEDIATE viewCreateStmt;

			CalcEntropy(newTableNode.viewName, newTableNode.entropy, newTableNode.rowTotal);
			CalcGiniIndex(newTableNode.viewName, newTableNode.giniIndex, newTableNode.rowTotal);
			CalcClassificationError(newTableNode.viewName, newTableNode.classificationErr, newTableNode.rowTotal);

			infoGain_Ent := infoGain_Ent - (newTableNode.rowTotal / curRootNode.rowTotal) * newTableNode.entropy;
			infoGain_Gini := infoGain_Gini - (newTableNode.rowTotal / curRootNode.rowTotal) * newTableNode.giniIndex;
			infoGain_ClsErr := infoGain_ClsErr - (newTableNode.rowTotal / curRootNode.rowTotal) * newTableNode.classificationErr;
			-- add this new table to the list
			newNodeList.EXTEND;
			newNodeList(newNodeList.COUNT) := newTableNode;
		END LOOP; -- end loop for each distinct value of current attribute
		CLOSE attrValues;
		infoGain_Ent := ROUND(infoGain_Ent,3);
		infoGain_Gini := ROUND(infoGain_Gini,3);
		infoGain_ClsErr := ROUND(infoGain_ClsErr,3);
		curCandidateAttr.attrName := curAttrName;
		curCandidateAttr.infoGain_Ent := infoGain_Ent;
		curCandidateAttr.infoGain_Gini := infoGain_Gini;
		curCandidateAttr.infoGain_ClsErr := infoGain_ClsErr;
		curCandidateAttr.tableNodes := newNodeList;

		-- add current candidate attribute's info to the candidate list
		candidateAttrs.EXTEND;
		candidateAttrs(candidateAttrs.COUNT) := curCandidateAttr;
		-- clear
		newNodeList.TRIM(newNodeList.COUNT);
	END LOOP; -- end loop for each attribute column
	
	-- find the max info gain
	FOR i IN candidateAttrs.FIRST..candidateAttrs.LAST
	LOOP
		IF candidateAttrs(i).infoGain_ClsErr > maxInfoGain_ClsErr THEN
			maxInfoGain_ClsErr := candidateAttrs(i).infoGain_ClsErr;
			chosenAttr := candidateAttrs(i);
		END IF;
	END LOOP;
	
	-- if max info gain is 0, try next method, normally this is not happenning
	IF maxInfoGain_ClsErr = 0 THEN
		FOR i IN candidateAttrs.FIRST..candidateAttrs.LAST
		LOOP
			IF candidateAttrs(i).infoGain_Ent > maxInfoGain_Ent THEN
				maxInfoGain_Ent := candidateAttrs(i).infoGain_Ent;
				chosenAttr := candidateAttrs(i);
			END IF;
		END LOOP;
	END IF;
	
	IF maxInfoGain_Ent = 0 THEN 
		FOR i IN candidateAttrs.FIRST..candidateAttrs.LAST
		LOOP
			IF candidateAttrs(i).infoGain_Gini > maxInfoGain_Gini THEN
				maxInfoGain_Gini := candidateAttrs(i).infoGain_Gini;
				chosenAttr := candidateAttrs(i);
			END IF;
		END LOOP;
	END IF;
	
	-- recursively call generateDT on each table node of resulting from the chosen attribute
	DBMS_OUTPUT.PUT_LINE('chosen attr: ' || chosenAttr.attrName);
	IF chosenAttr.tableNodes.COUNT > 0 THEN
		FOR j IN chosenAttr.tableNodes.FIRST..chosenAttr.tableNodes.LAST
		LOOP
			GenerateDT(chosenAttr.tableNodes(j));
		END LOOP;
	END IF;
	CLOSE attrNames;
	candidateAttrs.TRIM(candidateAttrs.COUNT);
END;

-- The main procedure
BEGIN
	-- Init with the root parent table
	EXECUTE IMMEDIATE
    'CREATE OR REPLACE VIEW ' || rootView || ' AS SELECT * FROM ' || table_name;
	rootNode.viewName := rootView;

	CalcEntropy(rootNode.viewName, rootNode.entropy, rootNode.rowTotal);
	CalcGiniIndex(rootNode.viewName, rootNode.giniIndex, rootNode.rowTotal);
	CalcClassificationError(rootNode.viewName, rootNode.classificationErr, rootNode.rowTotal);

	vi := 0;
	GenerateDT(rootNode);
	IF resultRules.COUNT > 0 THEN
		FOR i IN resultRules.FIRST..resultRules.LAST
		LOOP
			DBMS_OUTPUT.PUT_LINE('rule: ' || resultRules(i));
		END LOOP;
	END IF;

END CreateDT;
/

exec createdt('trans', 'transmode');