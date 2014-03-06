package bigIntAdd;

public class BigIntAdd {
	static int [] stringToInt(String s1) {
		char []s = s1.toCharArray();
		int []bigint = new int[s.length];
		for(int i = 0; i < s.length; i ++)
		{
			String ss = String.valueOf(s[i]);
			bigint[i] = Integer.parseInt(ss);
		}
		return bigint;
	}
	static int [] calSum(int []a, int []b) {
		int k = a.length > b.length? a.length:b.length;
		//k ++;
		int max = k;
		int sum[] = new int[k];
		int tag = 0;
		int j = b.length - 1;
		for(int i = a.length-1; max > 0;i--, j--, max--)
		{
			if(j != -1&& i != -1){
				//System.out.print(a[i]);
				if(((a[i] + b[j] + tag) / 10)> 0) {
					sum[--k] = (a[i] + b[j] + tag) % 10;
					//System.out.println(sum[k]);
					tag = 1;
				}
				else {
					sum[--k] = (a[i] + b[j] + tag) % 10;
					//System.out.println(sum[k]);
					tag = 0;
				}
			}
			else {
				if(i == -1) {
					if((b[j] + tag) == 10) {
						sum[--k] = 0;
						//System.out.println(sum[k]);
						tag = 1;
						i++;
					}
					else{
						sum[--k] = b[j] + tag;
						//System.out.println(sum[k]);
						tag = 0;
						i++;
					}
				}
				else {
					if((a[i] + tag) == 10) {
						sum[k--] = 0;
					//	System.out.println(sum[k]);
						tag = 1;
						j++;
						
					}
					else{
						sum[k--] = a[i] + tag;
						//System.out.println(sum[k]);
						tag = 0;
						j++;
					}

				}
			}
		}
		sum[0] = tag;
		return sum;
	}
	public static void main(String []args) {
		String str1 = "123456789012345";
		String str2 = "98765433434";
		int a[] = stringToInt(str1);
		int b[] = stringToInt(str2);
		int c[] = calSum(a, b);
		System.out.print(str1 + '+' +  str2 + '=');
		
		for(int i = 0; i < c.length; i ++)
			System.out.print(c[i]);

	}

}
