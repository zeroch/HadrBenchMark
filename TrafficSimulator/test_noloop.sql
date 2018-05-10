
	UPDATE Production.Product  
		SET ListPrice = (select ROUND(RAND()*500, 0))
		where ProductID = (select ROUND(RAND()*1000, 0))

	update Production.Product
		set rowguid = NEWID()
		where ProductID = (select ROUND(RAND()*1000, 0))




	--DECLARE @cnt INT = 0;
	--while @cnt < 10
	--begin
		declare @seed int;
		declare @string varchar(128);

		select @seed = RAND()*1000; -- saved start seed


		exec usp_generateIdentifier 
			@seed = @seed output
			, @string = @string output;

		update Person.Password
		set PasswordHash = @string
			where BusinessEntityID = (select ROUND(RAND()*20777, 0));
	--	set @cnt = @cnt +1;
	--end



	--SET @cnt = 0;
	--while @cnt < 10
	--begin
		declare @newcard varchar(25)
		-- all numbers between 0 and 9
		exec [dbo].uspRandChars @len=16, @min=48, @range=9, @exclude='', @output=@newcard out

		update Sales.CreditCard
			set CardNumber = @newcard
			where CreditCardID = (select ROUND(RAND()*18000, 0))
	--	set @cnt = @cnt +1;
	--end





