public class UserProfile
{
    public string UserId { get; set; }
    public string Email { get; set; }
    public int Age { get; set; }
}


/*
 Homework: 

1. Insert 1000 user profiles into the DynamoDB table with unique UserIds, Emails, and Ages.
2. Retrieve the details of 5 user profiles based on a unique UserId.
3. Test the query and scan methods on the DynamoDB table to filter users by Age greater than 25.Insert additional data and measure performance
4. Add advanced models such as Address (with fields like Street, City, ZipCode) and Preferences (with fields like NewsletterSubscribed, NotificationsEnabled) to the UserProfile class. 
 */

