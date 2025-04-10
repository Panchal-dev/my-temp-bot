import requests
import time
from bs4 import BeautifulSoup
import re

class AdrinoBypass:
    def __init__(self, short_url):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })
        self.short_url = short_url
        self.base_url = "https://keedabankingnews.com"
        self.final_url = None
    
    def get_hidden_input(self, soup, name):
        input_tag = soup.find('input', {'name': name})
        return input_tag['value'] if input_tag else None
    
    def close_popup(self, soup):
        popup = soup.find('div', {'class': 'popup'})
        if popup and 'display: block;' in popup.get('style', '').lower():
            print("Closing popup ad...")
            return True
        return False
    
    def handle_countdown(self, soup):
        countdown_div = soup.find('button', {'id': 'countdown'}) or soup.find('div', {'id': 'countdown'})
        if countdown_div:
            seconds = 10 # Default wait time
            time_span = countdown_div.find('span', {'id': 'tp-time'})
            if time_span:
                try:
                    seconds = int(time_span.text)
                except ValueError:
                    pass
            print(f"Waiting for {seconds} seconds...")
            time.sleep(seconds)
            return True
        return False
    
    def process_page(self, url, step, expected_step, form_data_name=None):
        print(f"\nProcessing step {step}/{expected_step} at {url}")
        
        # Get the page with a timeout
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error accessing page: {e}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Verify we're on the right step
        step_text = soup.find(string=re.compile(f"You are currently on step {step}/{expected_step}"))
        if not step_text:
            print("Step verification failed. Continuing anyway...")
        
        # Handle countdown
        self.handle_countdown(soup)
        
        # Close popup if present
        self.close_popup(soup)
        
        # On the last step, look for the Get Link button
        if step == expected_step:
            get_link = soup.find('a', href=re.compile(r'/includes/open\.php'))
            if get_link:
                final_path = get_link['href']
                if not final_path.startswith('http'):
                    final_path = f"{self.base_url}{final_path}"
                self.final_url = final_path
                print("Found final link path:", self.final_url)
                
                # Simulate clicking the Get Link button
                try:
                    final_response = self.session.get(self.final_url, allow_redirects=False, timeout=10)
                    if final_response.status_code in [301, 302, 303, 307, 308]:
                        telegram_url = final_response.headers['Location']
                        print("\nSuccess! Final Telegram URL:", telegram_url)
                        return telegram_url
                except requests.exceptions.RequestException as e:
                    print(f"Error accessing final link: {e}")
                
                return None
        
        # For other steps, find the form to proceed
        form = soup.find('form', {'name': 'tp'})
        if not form:
            print("No form found to proceed")
            return None
        
        # Prepare form data
        form_data = {}
        if form_data_name:
            hidden_value = self.get_hidden_input(soup, form_data_name)
            if hidden_value:
                form_data[form_data_name] = hidden_value
        
        # Submit the form
        action_url = form['action']
        if not action_url.startswith('http'):
            action_url = f"{self.base_url}{action_url}"
        
        print("Proceeding to next step at:", action_url)
        try:
            response = self.session.post(action_url, data=form_data, allow_redirects=False, timeout=10)
            if response.status_code in [301, 302, 303, 307, 308]:
                return response.headers['Location']
            return response.url
        except requests.exceptions.RequestException as e:
            print(f"Error submitting form: {e}")
            return None
    
    def bypass(self):
        print(f"Starting bypass for {self.short_url}")
        
        # Step 1: Initial redirect from short URL
        try:
            response = self.session.get(self.short_url, allow_redirects=False, timeout=10)
            if response.status_code in [301, 302, 303, 307, 308]:
                current_url = response.headers['Location']
                print(f"Redirected to: {current_url}")
            else:
                current_url = self.short_url
        except requests.exceptions.RequestException as e:
            print(f"Error accessing short URL: {e}")
            return None
        
        # Process all 4 steps
        steps = [
            (1, 'tp2'),
            (2, 'tp3'),
            (3, 'tp4'),
            (4, None)  # Last step has no form data
        ]
        
        for step, form_data_name in steps:
            result = self.process_page(current_url, step, 4, form_data_name)
            
            if isinstance(result, str) and result.startswith('http'):
                current_url = result
            elif result is None:
                print("Failed to proceed to next step")
                return None
            elif step == 4 and result:  # Got the final Telegram URL
                return result
        
        print("Failed to extract final URL")
        return None

if __name__ == "__main__":
    # Example usage
    short_url = "https://adrinolinks.in/HucM6"  # Replace with your short URL
    bypasser = AdrinoBypass(short_url)
    final_url = bypasser.bypass()
    
    if final_url:
        print("\nBypass successful!")
        print("Final URL:", final_url)
    else:
        print("\nBypass failed.")